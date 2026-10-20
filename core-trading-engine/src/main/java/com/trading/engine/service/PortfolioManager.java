package com.trading.engine.service;

import com.trading.engine.dto.Position;
import com.trading.engine.dto.StockSignalDto;
import com.trading.engine.dto.TradeLog;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Service
public class PortfolioManager {

    @Value("${trading.initial-capital-krw:10000000}")
    private double initialCapitalKrw;

    @Value("${trading.usd-krw-rate:1350.0}")
    private double usdKrwRate;

    @Getter
    private double initialCashUsd;

    @Getter
    private double currentCashUsd;

    @Getter
    private double cumulativeRealizedPnlUsd = 0.0;

    // 보유 포지션 관리 (티커 -> Position)
    private final ConcurrentHashMap<String, Position> positions = new ConcurrentHashMap<>();

    // 거래 체결 히스토리 로그 (웹 대시보드 출력용)
    @Getter
    private final List<TradeLog> tradeLogs = new CopyOnWriteArrayList<>();

    @PostConstruct
    public void init() {
        this.initialCashUsd = initialCapitalKrw / usdKrwRate;
        this.currentCashUsd = this.initialCashUsd;

        log.info("==================================================");
        log.info(">> [1,000만원 가상 포트폴리오 트래커 초기화]");
        log.info("   초기 자본: ₩{} (약 ${} USD)",
                String.format(Locale.KOREA, "%,.0f", initialCapitalKrw),
                String.format(Locale.US, "%,.2f", initialCashUsd));
        log.info("==================================================");
    }

    /**
     * 포지션 매수 처리
     */
    public synchronized boolean buyPosition(StockSignalDto signal, int quantity, double price) {
        String ticker = signal.getTicker().toUpperCase();
        double totalCost = price * quantity;

        if (currentCashUsd < totalCost) {
            log.warn(">> [잔고 부족] {} 매수 불가. 필요금액: ${}, 보유현금: ${}",
                    ticker, String.format(Locale.US, "%.2f", totalCost), String.format(Locale.US, "%.2f", currentCashUsd));
            return false;
        }

        currentCashUsd -= totalCost;

        // 사용자 요청 전략: +3% 상승 시 익절, -6~7% 하락 시 손절
        double targetTakeProfit = (signal.getTakeProfit() != null && signal.getTakeProfit() > price)
                ? signal.getTakeProfit()
                : Math.round(price * 1.03 * 100.0) / 100.0; // 기본 +3% 익절

        double targetStopLoss = (signal.getStopLoss() != null && signal.getStopLoss() < price)
                ? signal.getStopLoss()
                : Math.round(price * 0.935 * 100.0) / 100.0; // 기본 -6.5% 손절

        Position existing = positions.get(ticker);
        if (existing != null) {
            int newQty = existing.getQuantity() + quantity;
            double newAvgPrice = ((existing.getBuyPrice() * existing.getQuantity()) + totalCost) / newQty;
            existing.setQuantity(newQty);
            existing.setBuyPrice(newAvgPrice);
            existing.setCurrentPrice(price);
            existing.setTakeProfitPrice(targetTakeProfit);
            existing.setStopLossPrice(targetStopLoss);
            log.info(">> [포지션 추가 매수] {} {}주 추가 체결 (총 보유: {}주, 평단가: ${})",
                    ticker, quantity, newQty, String.format(Locale.US, "%.2f", newAvgPrice));
        } else {
            Position newPos = Position.builder()
                    .ticker(ticker)
                    .stockName(signal.getStockName())
                    .exchange(signal.getExchange())
                    .quantity(quantity)
                    .buyPrice(price)
                    .currentPrice(price)
                    .takeProfitPrice(targetTakeProfit)
                    .stopLossPrice(targetStopLoss)
                    .buyReason(signal.getReason())
                    .buyTime(LocalDateTime.now())
                    .build();
            positions.put(ticker, newPos);
            log.info(">> [신규 포지션 진입] {} {}주 매수 체결 @ ${} (익절목표가: ${}, 손절가: ${})",
                    ticker, quantity, String.format(Locale.US, "%.2f", price),
                    String.format(Locale.US, "%.2f", targetTakeProfit),
                    String.format(Locale.US, "%.2f", targetStopLoss));
        }

        // 거래 히스토리 기록
        tradeLogs.add(0, TradeLog.builder()
                .id(UUID.randomUUID().toString().substring(0, 8))
                .ticker(ticker)
                .stockName(signal.getStockName())
                .action("BUY")
                .quantity(quantity)
                .price(price)
                .profitRate(0.0)
                .realizedPnl(0.0)
                .reason(signal.getReason())
                .timestamp(LocalDateTime.now())
                .build());

        printDashboard();
        return true;
    }

    /**
     * 포지션 매도 처리 (익절 / 손절 / AI 악재 긴급 매도)
     */
    public synchronized boolean sellPosition(String ticker, double executionPrice, String reason) {
        Position pos = positions.remove(ticker.toUpperCase());
        if (pos == null) {
            return false;
        }

        double totalRevenue = executionPrice * pos.getQuantity();
        double profitUsd = (executionPrice - pos.getBuyPrice()) * pos.getQuantity();
        double profitRate = ((executionPrice - pos.getBuyPrice()) / pos.getBuyPrice()) * 100.0;

        currentCashUsd += totalRevenue;
        cumulativeRealizedPnlUsd += profitUsd;

        log.info("==================================================");
        log.info(">> 🎯 [포지션 매도 체결] {} {}주 매도 완료 (@ ${})",
                ticker, pos.getQuantity(), String.format(Locale.US, "%.2f", executionPrice));
        log.info("   매도 사유: {}", reason);
        log.info("   매수 평단가: ${} -> 매도 단가: ${}",
                String.format(Locale.US, "%.2f", pos.getBuyPrice()), String.format(Locale.US, "%.2f", executionPrice));
        log.info("   실현 손익: {}${:.2f} ({}{:.2f}%) | ₩약 {:,}원",
                profitUsd >= 0 ? "+" : "", profitUsd,
                profitRate >= 0 ? "+" : "", profitRate,
                (long)(profitUsd * usdKrwRate));
        log.info("==================================================");

        // 거래 히스토리 기록
        tradeLogs.add(0, TradeLog.builder()
                .id(UUID.randomUUID().toString().substring(0, 8))
                .ticker(ticker)
                .stockName(pos.getStockName())
                .action("SELL")
                .quantity(pos.getQuantity())
                .price(executionPrice)
                .profitRate(profitRate)
                .realizedPnl(profitUsd)
                .reason(reason)
                .timestamp(LocalDateTime.now())
                .build());

        printDashboard();
        return true;
    }

    public Collection<Position> getAllPositions() {
        return positions.values();
    }

    public Position getPosition(String ticker) {
        return positions.get(ticker.toUpperCase());
    }

    public boolean hasPosition(String ticker) {
        return positions.containsKey(ticker.toUpperCase());
    }

    /**
     * 웹 대시보드 API용 데이터 종합 패키징
     */
    public Map<String, Object> getDashboardData() {
        double stockValuationUsd = 0.0;
        List<Map<String, Object>> positionList = new ArrayList<>();

        for (Position p : positions.values()) {
            double valuation = p.getCurrentPrice() * p.getQuantity();
            stockValuationUsd += valuation;

            Map<String, Object> posMap = new HashMap<>();
            posMap.put("ticker", p.getTicker());
            posMap.put("stockName", p.getStockName() != null ? p.getStockName() : p.getTicker());
            posMap.put("exchange", p.getExchange());
            posMap.put("quantity", p.getQuantity());
            posMap.put("buyPrice", p.getBuyPrice());
            posMap.put("currentPrice", p.getCurrentPrice());
            posMap.put("takeProfitPrice", p.getTakeProfitPrice());
            posMap.put("stopLossPrice", p.getStopLossPrice());
            posMap.put("profitRate", Math.round(p.getProfitRate() * 100.0) / 100.0);
            posMap.put("unrealizedPnl", Math.round(p.getUnrealizedPnl() * 100.0) / 100.0);
            posMap.put("buyReason", p.getBuyReason());
            posMap.put("buyTime", p.getBuyTime() != null ? p.getBuyTime().toString() : "");
            positionList.add(posMap);
        }

        double totalAssetUsd = currentCashUsd + stockValuationUsd;
        double totalAssetKrw = totalAssetUsd * usdKrwRate;
        double totalReturnRate = ((totalAssetUsd - initialCashUsd) / initialCashUsd) * 100.0;

        Map<String, Object> data = new HashMap<>();
        data.put("initialCapitalKrw", initialCapitalKrw);
        data.put("initialCashUsd", Math.round(initialCashUsd * 100.0) / 100.0);
        data.put("currentCashUsd", Math.round(currentCashUsd * 100.0) / 100.0);
        data.put("currentCashKrw", Math.round(currentCashUsd * usdKrwRate));
        data.put("stockValuationUsd", Math.round(stockValuationUsd * 100.0) / 100.0);
        data.put("stockValuationKrw", Math.round(stockValuationUsd * usdKrwRate));
        data.put("totalAssetUsd", Math.round(totalAssetUsd * 100.0) / 100.0);
        data.put("totalAssetKrw", Math.round(totalAssetKrw));
        data.put("totalReturnRate", Math.round(totalReturnRate * 100.0) / 100.0);
        data.put("cumulativeRealizedPnlUsd", Math.round(cumulativeRealizedPnlUsd * 100.0) / 100.0);
        data.put("cumulativeRealizedPnlKrw", Math.round(cumulativeRealizedPnlUsd * usdKrwRate));
        data.put("usdKrwRate", usdKrwRate);
        data.put("positions", positionList);
        data.put("tradeLogs", tradeLogs);

        return data;
    }

    /**
     * 콘솔 대시보드 출력 (포맷 스트링 오류 완벽 수정)
     */
    public void printDashboard() {
        double stockValuationUsd = 0.0;
        for (Position p : positions.values()) {
            stockValuationUsd += p.getCurrentPrice() * p.getQuantity();
        }

        double totalAssetUsd = currentCashUsd + stockValuationUsd;
        double totalAssetKrw = totalAssetUsd * usdKrwRate;
        double totalReturnRate = ((totalAssetUsd - initialCashUsd) / initialCashUsd) * 100.0;

        log.info("┌─────────────────────────────────────────────────────────────┐");
        log.info("│ 📊 [1,000만원 가상 포트폴리오 실시간 현황]                 │");
        log.info("│ - 총 평가 자산: ₩{} (약 ${}) [{}{}%] │",
                String.format(Locale.KOREA, "%,10.0f", totalAssetKrw),
                String.format(Locale.US, "%,9.2f", totalAssetUsd),
                totalReturnRate >= 0 ? "+" : "",
                String.format(Locale.US, "%.2f", totalReturnRate));
        log.info("│ - 보유 현금   : ${} (₩{})          │",
                String.format(Locale.US, "%,9.2f", currentCashUsd),
                String.format(Locale.KOREA, "%,10.0f", currentCashUsd * usdKrwRate));
        log.info("│ - 보유 주식   : ${} ({}개 종목 보유 중)           │",
                String.format(Locale.US, "%,9.2f", stockValuationUsd), positions.size());
        log.info("│ - 누적 실현손익: {}${:.2f} (₩{:,}원)            │",
                cumulativeRealizedPnlUsd >= 0 ? "+" : "", cumulativeRealizedPnlUsd,
                (long)(cumulativeRealizedPnlUsd * usdKrwRate));

        if (!positions.isEmpty()) {
            log.info("├─────────────────────────────────────────────────────────────┤");
            log.info("│ [보유 종목 리스트]                                         │");
            for (Position p : positions.values()) {
                log.info("│  * {} ({}주): 매수가 ${} | 현재가 ${} | 손익 {}{}% (익절목표: ${}, 손절: ${})",
                        p.getTicker(), p.getQuantity(),
                        String.format(Locale.US, "%.2f", p.getBuyPrice()),
                        String.format(Locale.US, "%.2f", p.getCurrentPrice()),
                        p.getProfitRate() >= 0 ? "+" : "",
                        String.format(Locale.US, "%.2f", p.getProfitRate()),
                        String.format(Locale.US, "%.2f", p.getTakeProfitPrice()),
                        String.format(Locale.US, "%.2f", p.getStopLossPrice()));
            }
        }
        log.info("└─────────────────────────────────────────────────────────────┘");
    }
}
