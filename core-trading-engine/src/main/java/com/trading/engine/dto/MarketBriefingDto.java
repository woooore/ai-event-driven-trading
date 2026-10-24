package com.trading.engine.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class MarketBriefingDto {

    private String id;
    private String date; // "2026. 10. 24." or "2026-10-24"
    private String time; // "06:00", "05:00", "04:00" 등 1시간 단위
    private String title; // "금리 상승에 다우 약세, 반도체만 버틴 혼조 마감"
    private List<String> bullets; // 3~4줄 요약 문장 리스트
    private String category; // "호재", "악재", "혼조", "글로벌 마켓"
    private Long timestamp; // 생성 타임스탬프 (밀리초)
}
