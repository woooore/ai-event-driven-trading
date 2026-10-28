package com.trading.engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.engine.dto.Position;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Locale;

@Slf4j
@Component
@EnableScheduling
@RequiredArgsConstructor
public class PositionMonitorScheduler {

    private final PortfolioManager portfolioManager;
    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${trading.take-profit-ratio:0.03}")
    private double defaultTakeProfitRatio; // 기본 목표선

    @Value("${trading.trailing-activation-ratio:0.025}")
    private double trailingActivationRatio; // +2.5% 수익 도달 시 트레일링 스탑 모드 가동

    @Value("${trading.trailing-pullback-ratio:0.030}")
    private double trailingPullbackRatio;   // 고점 대비 -3.0% 하락 시 추세 반전으로 익절 청산

    @Value("${trading.stop-loss-ratio:0.065}")
    private double defaultStopLossRatio;   // 기본 6.5% 하락 시 안전 손절

    @Value("${trading.max-hold-hours:48}")
    private long maxHoldHours;              // 뉴스 모멘텀 유효기간 (48시간 경과 후 횡보 시 타임 컷)

    private static final ZoneId KST_ZONE = ZoneId.of("Asia/Seoul");

    /**
     * 15초마다 보유 포지션의 실시간 현재가를 감시하고
     * 트레일링 스탑, 타임 컷, 안전 손절 조건 검사 및 자동 체결
     */
    @Scheduled(fixedDelay = 15000)
    public void monitorPositions() {
        Collection<Position> positions = portfolioManager.getAllPositions();
        if (positions.isEmpty()) {
            return;
        }

        for (Position pos : positions) {
            String ticker = pos.getTicker();
            double livePrice = fetchLivePrice(ticker);

            if (livePrice <= 0) {
                continue;
            }

            pos.setCurrentPrice(livePrice);
            double profitRate = pos.getProfitRate();

            // 최고점 갱신 (트레일링 스탑 기준가)
            if (pos.getHighestPrice() <= 0) {
                pos.setHighestPrice(pos.getBuyPrice());
            }
            if (livePrice > pos.getHighestPrice()) {
                pos.setHighestPrice(livePrice);
            }

            // 0. 목표 익절선 (+3.0%) 도달 시 즉시 확정 익절 매도! (3% 넘으면 즉시 차익 실현)
            double targetTakeProfit = pos.getTakeProfitPrice() > 0 ? pos.getTakeProfitPrice() : (pos.getBuyPrice() * (1.0 + defaultTakeProfitRatio));
            if (livePrice >= targetTakeProfit) {
                log.info(">> 🎯 [목표 익절선 도달 즉시 청산] {} 현재가 ${} >= 목표가 ${} (달성 수익률: +{:.2f}%)",
                        ticker, String.format(Locale.US, "%.2f", livePrice),
                        String.format(Locale.US, "%.2f", targetTakeProfit), profitRate);
                portfolioManager.sellPosition(ticker, livePrice,
                        String.format(Locale.US, "목표 익절선 ($%.2f) 돌파 확정 이익 실현 (수익률: +%.2f%%)", targetTakeProfit, profitRate));
                continue;
            }

            // 1. 트레일링 스탑 활성화 검사 (+2.5% 이상 수익 시 가동 시작)
            if (!pos.isTrailingStopActive() && livePrice >= pos.getBuyPrice() * (1.0 + trailingActivationRatio)) {
                pos.setTrailingStopActive(true);
                double initialTrailingPrice = pos.getHighestPrice() * (1.0 - trailingPullbackRatio);
                pos.setTrailingStopPrice(initialTrailingPrice);
                log.info(">> 🎯 [트레일링 스탑 가동] {} 수익률 +{:.2f}% 달성! 고점 추적 익절 모드 진입 (최초 트레일링 기준선: ${})",
                        ticker, profitRate, String.format(Locale.US, "%.2f", initialTrailingPrice));
            }

            // 2. 트레일링 스탑 익절 청산 검사 (고점 대비 -3.0% 되돌림 터치 시)
            if (pos.isTrailingStopActive()) {
                double updatedTrailingPrice = pos.getHighestPrice() * (1.0 - trailingPullbackRatio);
                if (updatedTrailingPrice > pos.getTrailingStopPrice()) {
                    pos.setTrailingStopPrice(updatedTrailingPrice);
                }

                if (livePrice <= pos.getTrailingStopPrice()) {
                    log.info(">> 🚀 [트레일링 스탑 고점 추적 익절] {} 현재가 ${} <= 트레일링선 ${} (최고가 ${} 대비 -{:.1f}%, 최종수익률: +{:.2f}%)",
                            ticker, String.format(Locale.US, "%.2f", livePrice),
                            String.format(Locale.US, "%.2f", pos.getTrailingStopPrice()),
                            String.format(Locale.US, "%.2f", pos.getHighestPrice()),
                            trailingPullbackRatio * 100, profitRate);

                    portfolioManager.sellPosition(ticker, livePrice,
                            String.format(Locale.US, "고점 추적 트레일링 스탑 익절 (최고가 $%.2f 대비 -%.1f%% 되돌림 청산, 최종 수익률: +%.2f%%)",
                                    pos.getHighestPrice(), trailingPullbackRatio * 100, profitRate));
                    continue;
                }
            }

            // 3. 타임 컷 (Time-Horizon Exit: 48시간 이상 보유 후 횡보 지속 시 자본 회전율 확보 매도)
            if (pos.getBuyTime() != null) {
                long hoursHeld = java.time.Duration.between(pos.getBuyTime(), LocalDateTime.now(KST_ZONE)).toHours();
                if (hoursHeld >= maxHoldHours && Math.abs(profitRate) <= 1.5) {
                    log.info(">> ⏱️ [타임 컷 모멘텀 소멸 청산] {} 보유시간 {}시간 경과 후 횡보(수익률 {:.2f}%). 현금 회전율 확보를 위해 매도.",
                            ticker, hoursHeld, profitRate);
                    portfolioManager.sellPosition(ticker, livePrice,
                            String.format(Locale.US, "이벤트 모멘텀 소멸 타임 컷 (%d시간 보유 후 횡보 청산, 수익률: %+.2f%%)", hoursHeld, profitRate));
                    continue;
                }
            }

            // 4. 안전 손절(Hard Stop-Loss) 조건 검사 (-6.5% 하락 시 즉각 자본 보호 매도)
            if (livePrice <= pos.getStopLossPrice()) {
                log.warn(">> ⚠️ [손절 기준선 터치] {} 현재가 ${} <= 손절가 ${} (손실률: {:.2f}%)",
                        ticker, String.format(Locale.US, "%.2f", livePrice),
                        String.format(Locale.US, "%.2f", pos.getStopLossPrice()), profitRate);
                portfolioManager.sellPosition(ticker, livePrice,
                        String.format(Locale.US, "손절선 이탈 방어 매도 (손실률: %.2f%%)", profitRate));
            }
        }
    }

    /**
     * 1분마다 계좌 잔고 및 포트폴리오 수익률 현황 브리핑 로그 출력
     */
    @Scheduled(fixedDelay = 60000)
    public void scheduledDashboardReport() {
        if (!portfolioManager.getAllPositions().isEmpty()) {
            portfolioManager.printDashboard();
        }
    }

    /**
     * Yahoo Finance 공개 API를 통해 실시간 현재가를 가볍게 조회
     */
    private double fetchLivePrice(String ticker) {
        String url = "https://query1.finance.yahoo.com/v8/finance/chart/" + ticker + "?interval=1m&range=1d";
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("User-Agent", "Mozilla/5.0");
            HttpEntity<String> entity = new HttpEntity<>(headers);

            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                JsonNode root = objectMapper.readTree(response.getBody());
                JsonNode meta = root.path("chart").path("result").get(0).path("meta");
                double regularMarketPrice = meta.path("regularMarketPrice").asDouble();
                if (regularMarketPrice > 0) {
                    return regularMarketPrice;
                }
            }
        } catch (Exception e) {
            // 네트워크 일시 오류 등
        }
        return -1.0;
    }
}
