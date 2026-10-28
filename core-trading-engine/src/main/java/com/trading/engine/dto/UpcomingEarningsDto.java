package com.trading.engine.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class UpcomingEarningsDto {
    private String ticker;              // 종목 티커 (예: AAPL)
    private String stockName;           // 종목명 (예: 애플 (Apple))
    private String expectedDate;        // 발표 예정 일자 (예: 2026-10-31)
    private String expectedTimeKst;     // 발표 예정 시각 (한국 시간, 예: 05:30 (KST))
    private String dDay;                // D-Day 라벨 (예: "D-2", "D-Day", "D-5")
    private int dDayNumber;             // D-Day 숫자 (정렬용)
    private String quarter;             // 대상 분기 (예: "2026 Q3")
    private String marketTiming;        // "장 마감 후 (AMC)" 또는 "장 시작 전 (BMO)"
    private String epsConsensus;        // 시장 예상 EPS 컨센서스 (예: "$1.60")
    private String revenueConsensus;    // 시장 예상 매출 컨센서스 (예: "$945억 달러")
    private String keyWatchPoints;      // 주요 관전 포인트 (예: "아이폰17 수요 및 AI 인프라")
    private String impliedVolatility;   // 실적 발표 옵션 내재 변동성 (예: "±5.8%")
}
