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
        int qty = defaultQuantity;
        String orderQty = String.valueOf(qty);

        // 단가 포맷 (USD 소수점 2자리)
        double price = signal.getOrderPrice() != null ? signal.getOrderPrice() : 0.0;
        String ordUnpr = String.format(Locale.US, "%.2f", price);

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