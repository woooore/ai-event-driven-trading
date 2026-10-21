package com.trading.engine.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
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
    private String action;          // BUY, SELL
    private String actionLabel;     // 한국어 라벨 (매수, 익절 매도, 손절 매도, 긴급 탈출 매도)
    private int quantity;
    private double price;
    private double profitRate;      // 매도 시 수익률 (%)
    private double realizedPnl;     // 실현 손익 (USD)
    private String reason;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime timestamp;
}

