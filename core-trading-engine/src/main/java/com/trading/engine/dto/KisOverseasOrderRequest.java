package com.trading.engine.dto;

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
public class KisOverseasOrderRequest {

    /**
     * 종합계좌번호 (8자리)
     */
    @JsonProperty("CANO")
    private String cano;

    /**
     * 계좌상품코드 (2자리, 보통 01)
     */
    @JsonProperty("ACNT_PRDT_CD")
    private String acntPrdtCd;

    /**
     * 해외거래소코드 (미국: NASD, NYSE, AMEX)
     */
    @JsonProperty("OVRS_EXCG_CD")
    private String ovrsExcgCd;

    /**
     * 상품번호 (미국 종목 심볼 티커, 예: NVDA, AAPL, TSLA)
     */
    @JsonProperty("PDNO")
    private String pdno;

    /**
     * 주문수량 (예: 1)
     */
    @JsonProperty("ORD_QTY")
    private String ordQty;

    /**
     * 해외주문단가 (예: "130.50", 0일 경우 시장가 성격이나 미장은 지정가 필수)
     */
    @JsonProperty("OVRS_ORD_UNPR")
    private String ovrsOrdUnpr;

    /**
     * 주문서버구분코드 (기본 "0")
     */
    @JsonProperty("ORD_SVR_DVSN_CD")
    @Builder.Default
    private String ordSvrDvsnCd = "0";

    /**
     * 주문구분 (00: 지정가)
     */
    @JsonProperty("ORD_DVSN")
    @Builder.Default
    private String ordDvsn = "00";
}
