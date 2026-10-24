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
public class EarningsEventDto {
    private String id;
    private String ticker;
    private String stockName;
    private String date;             // YYYY-MM-DD or YYYY. MM. dd.
    private String time;             // HH:mm:ss or HH:mm
    private String quarter;          // e.g. "2026 Q3", "2026 Q4", "연간 가이던스"
    private String eventType;        // "어닝 서프라이즈", "가이던스 상향", "실적 발표", "어닝 쇼크"
    private String headline;         // 원본 뉴스 헤드라인
    private String epsSummary;       // EPS 실적 요약 (예: "조정 EPS $3.42 (+61% YoY, 예상치 대폭 상회)")
    private String revenueSummary;   // 매출/가이던스 요약 (예: "연간 매출 40억 달러 상향 조정")
    private String aiSummary;        // AI 실적 분석 핵심 총평
    private String sentiment;        // "호재", "악재", "중립"
    private Double stockImpactPct;   // 당일 주가 변동률 (%)
    private Long timestamp;          // epoch ms
}
