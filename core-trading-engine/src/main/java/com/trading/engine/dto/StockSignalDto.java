package com.trading.engine.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class StockSignalDto {

    /**
     * 종목 심볼 티커 (예: NVDA, AAPL, TSLA)
     */
    @JsonProperty("ticker")
    @JsonAlias({"stock_code", "symbol"})
    private String ticker;

    /**
     * 종목명
     */
    @JsonProperty("stock_name")
    @JsonAlias({"stockName", "name"})
    private String stockName;

    /**
     * 매매 액션: BUY, SELL, HOLD
     */
    @JsonProperty("action")
    @Builder.Default
    private String action = "BUY";

    /**
     * 미국 거래소 코드: NASD(나스닥), NYSE(뉴욕), AMEX(아멕스)
     */
    @JsonProperty("exchange")
    @Builder.Default
    private String exchange = "NASD";

    /**
     * 주문/진입 권장 단가 (USD)
     */
    @JsonProperty("order_price")
    @JsonAlias({"target_price", "orderPrice", "price"})
    private Double orderPrice;

    /**
     * AI 확신도 / 감성 점수 (0.0 ~ 1.0)
     */
    @JsonProperty("confidence")
    @JsonAlias({"score"})
    private Double confidence;

    /**
     * 손절 기준가 (USD)
     */
    @JsonProperty("stop_loss")
    @JsonAlias({"stopLoss"})
    private Double stopLoss;

    /**
     * 목표 익절가 (USD)
     */
    @JsonProperty("take_profit")
    @JsonAlias({"takeProfit"})
    private Double takeProfit;

    /**
     * 감성 분류 (POSITIVE, NEGATIVE, NEUTRAL)
     */
    private String sentiment;

    /**
     * AI 판단 근거 및 투자 논거
     */
    private String reason;

    // 호환성을 위한 게터
    public String getStockCode() {
        return ticker;
    }
}