package com.trading.engine.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class AnalysisLogDto {

    private String id;
    private String timestamp; // "2026-10-24 20:45:10"
    private String date;      // "2026-10-24"
    private String time;      // "20:45"
    private String ticker;
    
    @JsonProperty("stock_name")
    private String stockName;
    
    private String action; // BUY, SELL, HOLD
    private Double confidence;
    
    @JsonProperty("news_impact_score")
    private Integer newsImpactScore;
    
    @JsonProperty("priced_in_risk")
    private String pricedInRisk;
    
    @JsonProperty("position_size_pct")
    private Double positionSizePct;
    
    @JsonProperty("order_price")
    private Double orderPrice;
    
    @JsonProperty("pct_change")
    private Double pctChange;
    
    private String headline;
    private String reason;
    private Double rsi;
    
    @JsonProperty("ma_disparity")
    private Double maDisparity;
    
    @JsonProperty("bollinger_b")
    private Double bollingerB;
    
    @JsonProperty("quant_score")
    private Integer quantScore;
}
