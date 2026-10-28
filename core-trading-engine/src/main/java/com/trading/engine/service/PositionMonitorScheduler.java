package com.trading.engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.engine.dto.Position;
import jakarta.annotation.PostConstruct;
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
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@EnableScheduling
@RequiredArgsConstructor
public class PositionMonitorScheduler {

    private final PortfolioManager portfolioManager;
    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${trading.take-profit-ratio:0.03}")
    private double defaultTakeProfitRatio; // 기본 목표선 (+3.0%)

    @Value("${trading.trailing-activation-ratio:0.025}")
    private double trailingActivationRatio; // +2.5% 수익 도달 시 트레일링 스탑 모드 가동

    @Value("${trading.trailing-pullback-ratio:0.030}")
    private double trailingPullbackRatio;   // 고점 대비 -3.0% 하락 시 추세 반전으로 익절 청산

    @Value("${trading.stop-loss-ratio:0.065}")
    private double defaultStopLossRatio;   // 기본 -6.5% 하락 시 안전 손절

    @Value("${trading.max-hold-hours:48}")
    private long maxHoldHours;              // 뉴스 모멘텀 유효기간 (48시간 경과 후 횡보 시 타임 컷)

    private static final ZoneId KST_ZONE = ZoneId.of("Asia/Seoul");

    @PostConstruct
    public void onStartup() {
        // 엔진 기동 직후 비동기로 즉시 보유 종목 실시간 시세 1회 갱신
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(1500);
                monitorPositions();
            } catch (Exception ignored) {}
        });
    }

    /**
     * 10초마다 보유 포지션의 실시간 현재가(프리마켓/정규장/애프터마켓 포함)를 감시하고
     * 목표 익절선(+3.0%), 트레일링 스탑, 타임 컷, 안전 손절 조건 검사 및 자동 체결
     */
    @Scheduled(fixedDelay = 10000, initialDelay = 3000)
    public void monitorPositions() {
        Collection<Position> positions = portfolioManager.getAllPositions();
        if (positions.isEmpty()) {
            return;
        }

        boolean anyPriceUpdated = false;

        for (Position pos : positions) {
            String ticker = pos.getTicker();
            double livePrice = fetchLivePrice(ticker);

            if (livePrice <= 0) {
                continue;
            }

            if (Math.abs(pos.getCurrentPrice() - livePrice) >= 0.001) {
                anyPriceUpdated = true;
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

            // 0. 목표 익절선 (+3.0%) 도달 시 즉시 확정 익절 매도!
            double targetTakeProfit = pos.getTakeProfitPrice() > 0 ? pos.getTakeProfitPrice() : (pos.getBuyPrice() * (1.0 + defaultTakeProfitRatio));
            if (livePrice >= targetTakeProfit || profitRate >= 3.0) {
                log.info(">> 🎯 [목표 익절선 도달 즉시 청산] {} 현재가 ${} >= 목표가 ${} (달성 수익률: +{}%)",
                        ticker, String.format(Locale.US, "%.2f", livePrice),
                        String.format(Locale.US, "%.2f", targetTakeProfit),
                        String.format(Locale.US, "%.2f", profitRate));
                portfolioManager.sellPosition(ticker, livePrice,
                        String.format(Locale.US, "목표 익절선 ($%.2f) 돌파 확정 이익 실현 (수익률: +%.2f%%)", targetTakeProfit, profitRate));
                continue;
            }

            // 1. 트레일링 스탑 활성화 검사 (+2.5% 이상 수익 시 가동 시작)
            if (!pos.isTrailingStopActive() && livePrice >= pos.getBuyPrice() * (1.0 + trailingActivationRatio)) {
                pos.setTrailingStopActive(true);
                double initialTrailingPrice = pos.getHighestPrice() * (1.0 - trailingPullbackRatio);
                pos.setTrailingStopPrice(initialTrailingPrice);
                log.info(">> 🎯 [트레일링 스탑 가동] {} 수익률 +{}% 달성! 고점 추적 익절 모드 진입 (기준선: ${})",
                        ticker, String.format(Locale.US, "%.2f", profitRate), String.format(Locale.US, "%.2f", initialTrailingPrice));
            }

            // 2. 트레일링 스탑 익절 청산 검사 (고점 대비 -3.0% 되돌림 터치 시)
            if (pos.isTrailingStopActive()) {
                double updatedTrailingPrice = pos.getHighestPrice() * (1.0 - trailingPullbackRatio);
                if (updatedTrailingPrice > pos.getTrailingStopPrice()) {
                    pos.setTrailingStopPrice(updatedTrailingPrice);
                }

                if (livePrice <= pos.getTrailingStopPrice()) {
                    log.info(">> 🚀 [트레일링 스탑 고점 추적 익절] {} 현재가 ${} <= 트레일링선 ${} (최고가 ${} 대비 -{}%, 최종수익률: +{}%)",
                            ticker, String.format(Locale.US, "%.2f", livePrice),
                            String.format(Locale.US, "%.2f", pos.getTrailingStopPrice()),
                            String.format(Locale.US, "%.2f", pos.getHighestPrice()),
                            String.format(Locale.US, "%.1f", trailingPullbackRatio * 100),
                            String.format(Locale.US, "%.2f", profitRate));

                    portfolioManager.sellPosition(ticker, livePrice,
                            String.format(Locale.US, "고점 추적 트레일링 스탑 익절 (최고가 $%.2f 대비 -%.1f%% 되돌림 청산, 최종 수익률: +%.2f%%)",
                                    pos.getHighestPrice(), trailingPullbackRatio * 100, profitRate));
                    continue;
                }
            }

            // 3. 타임 컷 (48시간 이상 보유 후 횡보 지속 시 자본 회전율 확보 매도)
            if (pos.getBuyTime() != null) {
                long hoursHeld = java.time.Duration.between(pos.getBuyTime(), LocalDateTime.now(KST_ZONE)).toHours();
                if (hoursHeld >= maxHoldHours && Math.abs(profitRate) <= 1.5) {
                    log.info(">> ⏱️ [타임 컷 모멘텀 소멸 청산] {} 보유시간 {}시간 경과 후 횡보(수익률 {}%). 현금 회전율 확보 매도.",
                            ticker, hoursHeld, String.format(Locale.US, "%.2f", profitRate));
                    portfolioManager.sellPosition(ticker, livePrice,
                            String.format(Locale.US, "이벤트 모멘텀 소멸 타임 컷 (%d시간 보유 후 횡보 청산, 수익률: %+.2f%%)", hoursHeld, profitRate));
                    continue;
                }
            }

            // 4. 안전 손절(Hard Stop-Loss) 조건 검사 (-6.5% 하락 시 즉각 자본 보호 매도)
            if (livePrice <= pos.getStopLossPrice() || profitRate <= -6.5) {
                log.warn(">> ⚠️ [손절 기준선 터치] {} 현재가 ${} <= 손절가 ${} (손실률: {}%)",
                        ticker, String.format(Locale.US, "%.2f", livePrice),
                        String.format(Locale.US, "%.2f", pos.getStopLossPrice()),
                        String.format(Locale.US, "%.2f", profitRate));
                portfolioManager.sellPosition(ticker, livePrice,
                        String.format(Locale.US, "손절선 이탈 방어 매도 (손실률: %.2f%%)", profitRate));
            }
        }

        if (anyPriceUpdated) {
            portfolioManager.persistState();
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
     * Yahoo Finance API를 통해 프리마켓/정규장/애프터마켓 실시간 체결가를 정밀 조회
     */
    private double fetchLivePrice(String ticker) {
        // ADR 등 특수 티커 호환 처리
        String queryTicker = "SPCX".equalsIgnoreCase(ticker) ? "DXYZ" : ticker;
        String[] hosts = {"https://query2.finance.yahoo.com", "https://query1.finance.yahoo.com"};

        for (String host : hosts) {
            String url = host + "/v8/finance/chart/" + queryTicker + "?interval=1m&range=5d&includePrePost=true";
            try {
                HttpHeaders headers = new HttpHeaders();
                headers.set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36");
                headers.set("Accept", "application/json");
                HttpEntity<String> entity = new HttpEntity<>(headers);

                ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
                if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                    JsonNode root = objectMapper.readTree(response.getBody());
                    JsonNode resultNode = root.path("chart").path("result");
                    if (resultNode.isArray() && !resultNode.isEmpty()) {
                        JsonNode firstResult = resultNode.get(0);
                        JsonNode meta = firstResult.path("meta");

                        // 1순위: 프리마켓/정규장/애프터마켓 포함 가장 최근 1분봉 체결가 (실시간 변동 완벽 반영)
                        JsonNode quoteArray = firstResult.path("indicators").path("quote");
                        if (quoteArray.isArray() && !quoteArray.isEmpty()) {
                            JsonNode closeArray = quoteArray.get(0).path("close");
                            if (closeArray.isArray() && !closeArray.isEmpty()) {
                                for (int i = closeArray.size() - 1; i >= 0; i--) {
                                    JsonNode val = closeArray.get(i);
                                    if (val != null && !val.isNull() && val.isNumber() && val.asDouble() > 0) {
                                        return Math.round(val.asDouble() * 100.0) / 100.0;
                                    }
                                }
                            }
                        }

                        // 2순위: meta의 preMarketPrice / postMarketPrice / regularMarketPrice
                        if (meta.hasNonNull("preMarketPrice") && meta.get("preMarketPrice").asDouble() > 0) {
                            return Math.round(meta.get("preMarketPrice").asDouble() * 100.0) / 100.0;
                        }
                        if (meta.hasNonNull("postMarketPrice") && meta.get("postMarketPrice").asDouble() > 0) {
                            return Math.round(meta.get("postMarketPrice").asDouble() * 100.0) / 100.0;
                        }
                        double regularMarketPrice = meta.path("regularMarketPrice").asDouble();
                        if (regularMarketPrice > 0) {
                            return Math.round(regularMarketPrice * 100.0) / 100.0;
                        }
                    }
                }
            } catch (Exception ignored) {
                // 다음 호스트 폴백 시도
            }
        }
        return -1.0;
    }
}
