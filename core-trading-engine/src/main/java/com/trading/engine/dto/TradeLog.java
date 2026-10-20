package com.trading.engine.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDateTime;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TradeLog {
    private String id;
    private String ticker;
    private String stockName;
    private String action;          // BUY, SELL (TAKE_PROFIT, STOP_LOSS, BAD_NEWS)
    private int quantity;
    private double price;
    private double profitRate;      // 매도 시 수익률 (%)
    private double realizedPnl;     // 실현 손익 (USD)
    private String reason;
    private LocalDateTime timestamp;
}
