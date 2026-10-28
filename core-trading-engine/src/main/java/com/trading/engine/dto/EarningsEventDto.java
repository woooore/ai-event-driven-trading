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
    private String date;             // YYYY-MM-DD or YYYY. MM. dd. (한국 날짜 KST)
    private String time;             // HH:mm:ss (한국 시간 KST 기준)
    private String quarter;          // e.g. "2026 Q3", "2026 Q4", "연간 가이던스"
    private String eventType;        // "어닝 서프라이즈", "가이던스 상향", "실적 발표", "어닝 쇼크"
    private String headline;         // 원본 뉴스 헤드라인
    private String epsSummary;       // EPS 실적 요약 (예: "조정 EPS $3.42 (+61% YoY, 예상치 대폭 상회)")
    private String revenueSummary;   // 매출/가이던스 요약 (예: "연간 매출 40억 달러 상향 조정")
    private String aiSummary;        // AI 실적 분석 핵심 총평
    private String sentiment;        // "호재", "악재", "중립"
    private Double stockImpactPct;   // 당일 주가 변동률 (%)
    private Long timestamp;          // epoch ms

    // 🔍 클릭 시 상세 모달용 심층 팩트시트
    private String detailEpsActual;        // 실제 발표 EPS (예: "$7.15")
    private String detailEpsEstimate;      // 컨센서스 EPS (예: "$6.82")
    private String detailEpsSurprise;      // EPS 서프라이즈율 (예: "+4.84% Beat")
    private String detailRevActual;        // 실제 매출 (예: "$1,008억 달러")
    private String detailRevEstimate;      // 예상 매출 (예: "$998억 달러")
    private String detailRevGrowthYoY;     // 전년비 매출 성장률 (예: "+9.2% YoY")
    private String detailGuidance;         // 다음 분기/연간 가이던스 내용
    private String detailKeyDrivers;       // 호실적 또는 쇼크의 핵심 동인
    private String detailMarketReaction;   // 시간외/정규장 시장 반응 및 수급 분석
}
