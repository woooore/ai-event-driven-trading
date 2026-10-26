package com.trading.engine.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.trading.engine.dto.Position;
import com.trading.engine.dto.StockSignalDto;
import com.trading.engine.dto.TradeLog;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Service
public class PortfolioManager {

    @Value("${trading.initial-capital-krw:20000000}")
    private double initialCapitalKrw;

    @Getter
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

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Autowired(required = false)
    private RedissonClient redissonClient;

    private static final ZoneId KST_ZONE = ZoneId.of("Asia/Seoul");

    // 주요 미국 우량주 한국어 명칭 사전
    private static final Map<String, String> KOREAN_STOCK_NAMES = Map.ofEntries(
            Map.entry("AAPL", "애플 (Apple)"),
            Map.entry("NVDA", "엔비디아 (NVIDIA)"),
            Map.entry("MSFT", "마이크로소프트 (Microsoft)"),
            Map.entry("AMZN", "아마존 (Amazon)"),
            Map.entry("GOOGL", "알파벳 구글 (Alphabet)"),
            Map.entry("META", "메타 (Meta)"),
            Map.entry("TSLA", "테슬라 (Tesla)"),
            Map.entry("LLY", "일라이 릴리 (Eli Lilly)"),
            Map.entry("AVGO", "브로드컴 (Broadcom)"),
            Map.entry("AMD", "AMD (어드밴스드 마이크로)"),
            Map.entry("QCOM", "퀄컴 (Qualcomm)"),
            Map.entry("NFLX", "넷플릭스 (Netflix)"),
            Map.entry("ORCL", "오라클 (Oracle)"),
            Map.entry("CRM", "세일즈포스 (Salesforce)"),
            Map.entry("WMT", "월마트 (Walmart)"),
            Map.entry("JPM", "JP모건 체이스 (JPMorgan)"),
            Map.entry("UNH", "유나이티드헬스 (UnitedHealth)"),
            Map.entry("DIS", "월트 디즈니 (Disney)"),
            Map.entry("KO", "코카콜라 (Coca-Cola)"),
            Map.entry("COST", "코스트코 (Costco)")
    );

    public static String resolveKoreanName(String ticker, String fallbackName) {
        if (ticker != null && KOREAN_STOCK_NAMES.containsKey(ticker.toUpperCase())) {
            return KOREAN_STOCK_NAMES.get(ticker.toUpperCase());
        }
        return (fallbackName != null && !fallbackName.isBlank()) ? fallbackName : ticker;
    }

    @PostConstruct
    public void init() {
        this.initialCashUsd = initialCapitalKrw / usdKrwRate;
        this.currentCashUsd = this.initialCashUsd;

        // 도커 재시작/재빌드 시에도 종목 및 잔고가 초기화되지 않도록 영구 보존 데이터 복원 시도
        boolean restored = restorePersistedState();
        if (!restored) {
            log.info("==================================================");
            log.info(">> [2,000만원 가상 포트폴리오 트래커 초기화]");
            log.info("   초기 자본: ₩{} (약 ${} USD)",
                    String.format(Locale.KOREA, "%,.0f", initialCapitalKrw),
                    String.format(Locale.US, "%,.2f", initialCashUsd));
            log.info("==================================================");
        }
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
        String koreanName = resolveKoreanName(ticker, signal.getStockName());

        double targetTakeProfit = (signal.getTakeProfit() != null && signal.getTakeProfit() > price)
                ? signal.getTakeProfit()
                : Math.round(price * 1.03 * 100.0) / 100.0; // 기본 +3% 익절

        double targetStopLoss = (signal.getStopLoss() != null && signal.getStopLoss() < price)
                ? signal.getStopLoss()
                : Math.round(price * 0.935 * 100.0) / 100.0; // 기본 -6.5% 손절

        LocalDateTime nowKst = LocalDateTime.now(KST_ZONE);

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
                    koreanName, quantity, newQty, String.format(Locale.US, "%.2f", newAvgPrice));
        } else {
            Position newPos = Position.builder()
                    .ticker(ticker)
                    .stockName(koreanName)
                    .exchange(signal.getExchange())
                    .quantity(quantity)
                    .buyPrice(price)
                    .currentPrice(price)
                    .takeProfitPrice(targetTakeProfit)
                    .stopLossPrice(targetStopLoss)
                    .highestPrice(price)
                    .trailingStopActive(false)
                    .trailingStopPrice(0.0)
                    .buyReason(signal.getReason())
                    .buyTime(nowKst)
                    .build();
            positions.put(ticker, newPos);
            log.info(">> [신규 포지션 진입] {} {}주 매수 체결 @ ${} (트레일링스탑 발동기준: +2.5%, 손절가: ${})",
                    koreanName, quantity, String.format(Locale.US, "%.2f", price),
                    String.format(Locale.US, "%.2f", targetStopLoss));
        }

        // 거래 히스토리 기록 (한국 시간 및 한국어 라벨 적용)
        tradeLogs.add(0, TradeLog.builder()
                .id(UUID.randomUUID().toString().substring(0, 8))
                .ticker(ticker)
                .stockName(koreanName)
                .action("BUY")
                .actionLabel("매수 체결")
                .quantity(quantity)
                .price(price)
                .profitRate(0.0)
                .realizedPnl(0.0)
                .reason(signal.getReason())
                .timestamp(nowKst)
                .build());

        persistState();
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

        String koreanName = resolveKoreanName(ticker, pos.getStockName());
        LocalDateTime nowKst = LocalDateTime.now(KST_ZONE);

        String actionLabel = "매도 체결";
        if (reason != null && reason.contains("악재")) {
            actionLabel = "AI 긴급 매도";
        } else if (profitUsd > 0) {
            actionLabel = "익절 매도";
        } else if (profitUsd < 0) {
            actionLabel = "손절 매도";
        }

        log.info("==================================================");
        log.info(">> 🎯 [포지션 매도 체결] {} {}주 매도 완료 (@ ${}) [{}]",
                koreanName, pos.getQuantity(), String.format(Locale.US, "%.2f", executionPrice), actionLabel);
        log.info("   매도 사유: {}", reason);
        log.info("   매수 평단가: ${} -> 매도 단가: ${}",
                String.format(Locale.US, "%.2f", pos.getBuyPrice()), String.format(Locale.US, "%.2f", executionPrice));
        log.info("   실현 손익: {}${:.2f} ({}{:.2f}%) | ₩약 {:,}원",
                profitUsd >= 0 ? "+" : "", profitUsd,
                profitRate >= 0 ? "+" : "", profitRate,
                (long)(profitUsd * usdKrwRate));
        log.info("==================================================");

        // 거래 히스토리 기록 (한국 시간 및 한국어 라벨 적용)
        tradeLogs.add(0, TradeLog.builder()
                .id(UUID.randomUUID().toString().substring(0, 8))
                .ticker(ticker)
                .stockName(koreanName)
                .action("SELL")
                .actionLabel(actionLabel)
                .quantity(pos.getQuantity())
                .price(executionPrice)
                .profitRate(profitRate)
                .realizedPnl(profitUsd)
                .reason(reason)
                .timestamp(nowKst)
                .build());

        persistState();
        printDashboard();
        return true;
    }

    public Collection<Position> getAllPositions() {
        return positions.values();
    }

    /**
     * 총 자산(USD) = 현금 + 보유 종목 평가액 (비중 기반 매수 수량 산정용)
     */
    public double getTotalEquityUsd() {
        double holdings = 0.0;
        for (Position p : positions.values()) {
            double px = p.getCurrentPrice() > 0 ? p.getCurrentPrice() : p.getBuyPrice();
            holdings += px * p.getQuantity();
        }
        return currentCashUsd + holdings;
    }

    /**
     * 특정 종목의 현재 보유 평가액(USD)
     */
    public double getPositionValueUsd(String ticker) {
        Position p = positions.get(ticker.toUpperCase());
        if (p == null) return 0.0;
        double px = p.getCurrentPrice() > 0 ? p.getCurrentPrice() : p.getBuyPrice();
        return px * p.getQuantity();
    }

    public Position getPosition(String ticker) {
        return positions.get(ticker.toUpperCase());
    }

    public boolean hasPosition(String ticker) {
        return positions.containsKey(ticker.toUpperCase());
    }

    /**
     * 포트폴리오 전체 초기화 (설정된 초기 원금 복원 및 보유 종목/체결 로그 초기화)
     */
    public synchronized void resetPortfolio() {
        this.currentCashUsd = this.initialCashUsd;
        this.cumulativeRealizedPnlUsd = 0.0;
        this.positions.clear();
        this.tradeLogs.clear();

        if (redissonClient != null) {
            try {
                redissonClient.getBucket("portfolio:persisted_state").delete();
            } catch (Exception ignored) {}
        }
        try {
            Files.deleteIfExists(Path.of("portfolio_state_backup.json"));
        } catch (Exception ignored) {}

        persistState();
        log.info(">> 🔄 [포트폴리오 전체 초기화 완료] 초기 자본 ₩{} (${}) 원상 복구 완료",
                String.format(Locale.KOREA, "%,.0f", initialCapitalKrw),
                String.format(Locale.US, "%,.2f", initialCashUsd));
    }

    /**
     * 포트폴리오 상태 영구 보존 (Redis 및 로컬 파일 2중 백업)
     */
    public synchronized void persistState() {
        try {
            Map<String, Object> state = new HashMap<>();
            state.put("currentCashUsd", currentCashUsd);
            state.put("cumulativeRealizedPnlUsd", cumulativeRealizedPnlUsd);
            state.put("positions", new ArrayList<>(positions.values()));
            state.put("tradeLogs", new ArrayList<>(tradeLogs));

            String json = objectMapper.writeValueAsString(state);

            if (redissonClient != null) {
                try {
                    RBucket<String> bucket = redissonClient.getBucket("portfolio:persisted_state");
                    bucket.set(json);
                } catch (Exception e) {
                    log.warn("Redis 영구 저장 오류: {}", e.getMessage());
                }
            }

            try {
                Files.writeString(Path.of("portfolio_state_backup.json"), json, StandardCharsets.UTF_8);
            } catch (Exception ignored) {}

        } catch (Exception e) {
            log.warn("포트폴리오 영구 보존 직렬화 실패: {}", e.getMessage());
        }
    }

    /**
     * 포트폴리오 상태 영구 보존 데이터 복원
     */
    private boolean restorePersistedState() {
        try {
            String json = null;
            if (redissonClient != null) {
                try {
                    RBucket<String> bucket = redissonClient.getBucket("portfolio:persisted_state");
                    if (bucket.isExists()) {
                        json = bucket.get();
                        log.info(">> [Redis] 영구 보존된 이전 포트폴리오 데이터 발견");
                    }
                } catch (Exception e) {
                    log.warn(">> Redis 복원 확인 중 오류: {}", e.getMessage());
                }
            }

            Path backupPath = Path.of("portfolio_state_backup.json");
            if ((json == null || json.isBlank()) && Files.exists(backupPath)) {
                json = Files.readString(backupPath, StandardCharsets.UTF_8);
                log.info(">> [로컬 파일] 영구 보존된 이전 포트폴리오 백업 파일 발견");
            }

            if (json != null && !json.isBlank()) {
                JsonNode root = objectMapper.readTree(json);
                if (root.has("currentCashUsd")) {
                    this.currentCashUsd = root.get("currentCashUsd").asDouble();
                }
                if (root.has("cumulativeRealizedPnlUsd")) {
                    this.cumulativeRealizedPnlUsd = root.get("cumulativeRealizedPnlUsd").asDouble();
                }

                if (root.has("positions")) {
                    List<Position> posList = objectMapper.convertValue(root.get("positions"), new TypeReference<List<Position>>() {});
                    this.positions.clear();
                    for (Position p : posList) {
                        this.positions.put(p.getTicker().toUpperCase(), p);
                    }
                }

                if (root.has("tradeLogs")) {
                    List<TradeLog> logs = objectMapper.convertValue(root.get("tradeLogs"), new TypeReference<List<TradeLog>>() {});
                    this.tradeLogs.clear();
                    this.tradeLogs.addAll(logs);
                }

                log.info("==================================================");
                log.info(">> 💾 [포트폴리오 영구 보존 복원 완료]");
                log.info("   복원 보유 종목: {}개, 복원 잔고: ${}, 누적 손익: ${}",
                        positions.size(),
                        String.format(Locale.US, "%,.2f", currentCashUsd),
                        String.format(Locale.US, "%,.2f", cumulativeRealizedPnlUsd));
                log.info("==================================================");
                return true;
            }
        } catch (Exception e) {
            log.error(">> 이전 포트폴리오 데이터 복원 오류 (신규 초기화 진행): {}", e.getMessage());
        }
        return false;
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
     * 콘솔 대시보드 출력
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
                        p.getStockName(), p.getQuantity(),
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

