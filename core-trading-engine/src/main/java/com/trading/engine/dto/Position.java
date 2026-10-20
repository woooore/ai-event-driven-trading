package com.trading.engine.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Position {

    private String ticker;              // 종목 티커 (예: NVDA, AAPL)
    private String stockName;           // 종목명
    private String exchange;            // 거래소 (NASD, NYSE)
    private int quantity;               // 보유 수량
    private double buyPrice;            // 평균 매수 단가 (USD)
    private double currentPrice;        // 현재 시장 가격 (USD)
    private double takeProfitPrice;     // 목표 익절가 (USD)
    private double stopLossPrice;       // 손절가 (USD)
    private String buyReason;           // AI 매수 근거
    private LocalDateTime buyTime;      // 매수 시각

    /**
     * 현재 수익률 (%) 계산
     */
    public double getProfitRate() {
        if (buyPrice <= 0) return 0.0;
        return ((currentPrice - buyPrice) / buyPrice) * 100.0;
    }

    /**
     * 현재 평가 손익금 (USD) 계산
     */
    public double getUnrealizedPnl() {
        return (currentPrice - buyPrice) * quantity;
    }
}
