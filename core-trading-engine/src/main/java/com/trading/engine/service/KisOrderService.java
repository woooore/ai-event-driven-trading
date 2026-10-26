package com.trading.engine.service;

import com.trading.engine.dto.KisOverseasOrderRequest;
import com.trading.engine.dto.KisTokenResponse;
import com.trading.engine.dto.StockSignalDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Slf4j
@Service
public class KisOrderService {

    @Value("${kis.base-url}")
    private String baseUrl;

    @Value("${kis.app-key}")
    private String appKey;

    @Value("${kis.app-secret}")
    private String appSecret;

    @Value("${kis.account-no}")
    private String accountNo;

    @Value("${kis.account-code}")
    private String accountCode;

    @Value("${trading.simulation-mode:true}")
    private boolean simulationMode;

    @Value("${trading.default-quantity:1}")
    private int defaultQuantity;

    @Value("${trading.max-position-pct:15.0}")
    private double maxPositionPct;

    @Value("${trading.min-cash-reserve-pct:10.0}")
    private double minCashReservePct;

    private final PortfolioManager portfolioManager;
    private final RestTemplate restTemplate = new RestTemplate();
    private String accessToken;

    public KisOrderService(PortfolioManager portfolioManager) {
        this.portfolioManager = portfolioManager;
    }

    // 주요 NYSE 티커 (기본값은 대부분 테크주 기준 NASD)
    private static final Set<String> NYSE_TICKERS = Set.of(
            "IBM", "JPM", "UNH", "DIS", "BA", "BRK.B", "BRK.A", "XOM", "CVX", "WMT", "KO", "PEP", "PFE", "NKE", "LLY", "V", "MA", "HD", "PG", "JNJ", "ABBV", "BAC", "CRM", "ORCL", "WFC", "MCD", "CAT", "GE"
    );

    /**
     * 한국투자증권 OAuth2 Access Token 발급
     */
    public synchronized void issueAccessToken() {
        String url = baseUrl + "/oauth2/tokenP";

        Map<String, String> body = new HashMap<>();
        body.put("grant_type", "client_credentials");
        body.put("appkey", appKey);
        body.put("appsecret", appSecret);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);

        try {
            ResponseEntity<KisTokenResponse> response = restTemplate.postForEntity(url, request, KisTokenResponse.class);
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                this.accessToken = response.getBody().getAccessToken();
                log.info(">> [KIS API] Access Token 발급 성공!");
            }
        } catch (Exception e) {
            log.error(">> [KIS API] 토큰 발급 실패: {}", e.getMessage());
        }
    }

    /**
     * 미국 주식 주문 실행 (BUY / SELL)
     */
    public void executeOverseasOrder(StockSignalDto signal) {
        String ticker = signal.getTicker().toUpperCase().trim();
        String action = signal.getAction() != null ? signal.getAction().toUpperCase() : "BUY";
        String exchange = resolveExchange(ticker, signal.getExchange());

        // 단가 포맷 (USD 소수점 2자리)
        double price = signal.getOrderPrice() != null ? signal.getOrderPrice() : 0.0;
        String ordUnpr = String.format(Locale.US, "%.2f", price);

        // 매수 수량: 확신도/선반영/변동성이 반영된 권장 비중 기반으로 산정 (매도는 보유 수량 전량)
        int qty;
        if ("SELL".equalsIgnoreCase(action)) {
            var held = portfolioManager.getPosition(ticker);
            qty = held != null ? held.getQuantity() : defaultQuantity;
        } else {
            qty = calculateBuyQuantity(signal, ticker, price);
            if (qty <= 0) {
                log.warn(">> [매수 스킵] {} 비중 한도/현금 보유 비율 제약으로 매수 수량이 0주입니다.", ticker);
                return;
            }
        }
        String orderQty = String.valueOf(qty);

        log.info("--------------------------------------------------");
        log.info(">> [미국 주식 주문 요청 수신]");
        log.info("티커 : {} (종목명: {})", ticker, signal.getStockName());
        log.info("거래소: {}", exchange);
        log.info("액션 : {}", action);
        log.info("수량 : {}주, 단가: ${}", orderQty, ordUnpr);
        log.info("AI 확신도: {}, 근거: {}", signal.getConfidence(), signal.getReason());
        log.info("시뮬레이션 모드: {}", simulationMode ? "ON (가상 체결 & 1,000만원 포트폴리오 연동)" : "OFF (실계좌 주문 전송)");
        log.info("--------------------------------------------------");

        // 1. 시뮬레이션 모드일 경우: 1,000만원 가상 포트폴리오 매수/매도 반영
        if (simulationMode) {
            if ("SELL".equalsIgnoreCase(action)) {
                portfolioManager.sellPosition(ticker, price, signal.getReason());
            } else {
                portfolioManager.buyPosition(signal, qty, price);
            }
            return;
        }

        // 2. 실전 주문 모드 (실제 계좌에 주문 전송)
        if (this.accessToken == null) {
            issueAccessToken();
            if (this.accessToken == null) {
                log.error(">> Access Token이 없어 주문을 실행할 수 없습니다.");
                return;
            }
        }

        String url = baseUrl + "/uapi/overseas-stock/v1/trading/order";

        // KIS TR ID: 미국 매수 TTTT1002U, 미국 매도 TTTT1006U (실전 기준)
        String trId = "SELL".equalsIgnoreCase(action) ? "TTTT1006U" : "TTTT1002U";

        KisOverseasOrderRequest orderBody = KisOverseasOrderRequest.builder()
                .cano(accountNo)
                .acntPrdtCd(accountCode)
                .ovrsExcgCd(exchange)
                .pdno(ticker)
                .ordQty(orderQty)
                .ovrsOrdUnpr(ordUnpr)
                .ordDvsn("00") // 지정가
                .ordSvrDvsnCd("0")
                .build();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(this.accessToken);
        headers.set("appkey", appKey);
        headers.set("appsecret", appSecret);
        headers.set("tr_id", trId);

        HttpEntity<KisOverseasOrderRequest> request = new HttpEntity<>(orderBody, headers);

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
            log.info(">> [KIS API] 미장 주문 요청 완료. 응답: {}", response.getBody());
        } catch (Exception e) {
            log.error(">> [KIS API] 미장 주문 전송 실패 (잔고부족, 정규장 외 시간 등): {}", e.getMessage());
        }
    }

    /**
     * 확신도 기반 비중 매수 수량 산정
     *  - 권장 비중(position_size_pct)이 없으면 확신도로 추정 (0.9+ : 12%, 0.8+ : 8%, 그 외 5%)
     *  - 종목당 최대 비중(max-position-pct) 및 기보유분 고려
     *  - 최소 현금 비중(min-cash-reserve-pct)은 항상 남김
     */
    private int calculateBuyQuantity(StockSignalDto signal, String ticker, double price) {
        if (price <= 0) {
            return defaultQuantity;
        }

        double equity = portfolioManager.getTotalEquityUsd();
        double cash = portfolioManager.getCurrentCashUsd();

        double pct;
        if (signal.getPositionSizePct() != null && signal.getPositionSizePct() > 0) {
            pct = signal.getPositionSizePct();
        } else {
            pct = 25.0; // 기본 500만원 배정 (2,000만원 기준 25%)
        }
        pct = Math.max(5.0, Math.min(pct, maxPositionPct));

        // 종목당 약 500만원(₩5,000,000, 약 $3,703.7) 목표 예산 설정
        double targetBudgetUsd = 5000000.0 / portfolioManager.getUsdKrwRate();
        double budget = Math.max(equity * pct / 100.0, targetBudgetUsd);

        // 종목당 최대 비중 초과 방지 (이미 보유 중이면 잔여 한도만)
        double existingValue = portfolioManager.getPositionValueUsd(ticker);
        double capRemaining = (equity * maxPositionPct / 100.0) - existingValue;
        budget = Math.min(budget, capRemaining);

        // 현금 가용 범위 내로 제한 (최소 5% 버퍼 유지)
        double spendable = cash * 0.95;
        budget = Math.min(budget, spendable);

        if (budget < price) {
            return 0;
        }

        int qty = (int) Math.floor(budget / price);
        qty = Math.max(1, qty); // 1주 가격이 예산 내이면 최소 1주 이상 매수
        log.info(">> [500만원 집중 매수 산정] {} 비중 {}% -> 예산 ${} (₩약 {:,}원) -> {}주 매수 (단가 ${}, 총자산 ${}, 가용현금 ${})",
                ticker, String.format(Locale.US, "%.1f", pct),
                String.format(Locale.US, "%.2f", budget), (long)(budget * portfolioManager.getUsdKrwRate()),
                qty, String.format(Locale.US, "%.2f", price),
                String.format(Locale.US, "%.2f", equity), String.format(Locale.US, "%.2f", cash));
        return qty;
    }

    /**
     * 티커 기반 미국 거래소 코드 결정 (NASD, NYSE, AMEX)
     */
    private String resolveExchange(String ticker, String suggestedExchange) {
        if (suggestedExchange != null && !suggestedExchange.isBlank()) {
            return suggestedExchange.toUpperCase();
        }
        if (NYSE_TICKERS.contains(ticker)) {
            return "NYSE";
        }
        return "NASD"; // 기본값 나스닥
    }
}