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
    private double defaultTakeProfitRatio; // 기본 3% 상승 시 익절

    @Value("${trading.stop-loss-ratio:0.065}")
    private double defaultStopLossRatio;   // 기본 6.5% 하락 시 손절

    /**
     * 15초마다 보유 포지션의 실시간 현재가를 감시하고
     * 목표 익절가 또는 손절가 도달 시 자동으로 매도 체결
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

            // 1. 익절(Take-Profit) 조건 검사
            if (livePrice >= pos.getTakeProfitPrice()) {
                log.info(">> 🚀 [목표 익절 조건 달성] {} 현재가 ${} >= 익절목표가 ${} (수익률: +{:.2f}%)",
                        ticker, String.format(Locale.US, "%.2f", livePrice),
                        String.format(Locale.US, "%.2f", pos.getTakeProfitPrice()), profitRate);
                portfolioManager.sellPosition(ticker, livePrice, "목표 수익률 달성 익절 매도 (+ " + String.format(Locale.US, "%.2f", profitRate) + "%)");
                continue;
            }

            // 2. 손절(Stop-Loss) 조건 검사
            if (livePrice <= pos.getStopLossPrice()) {
                log.warn(">> ⚠️ [손절 기준선 터치] {} 현재가 ${} <= 손절가 ${} (손실률: {:.2f}%)",
                        ticker, String.format(Locale.US, "%.2f", livePrice),
                        String.format(Locale.US, "%.2f", pos.getStopLossPrice()), profitRate);
                portfolioManager.sellPosition(ticker, livePrice, "손절선 이탈 방어 매도 (" + String.format(Locale.US, "%.2f", profitRate) + "%)");
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
