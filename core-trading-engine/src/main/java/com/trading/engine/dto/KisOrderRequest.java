package com.trading.engine.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class KisOrderRequest {
    @JsonProperty("CANO")
    private String cano;         // 종합계좌번호(8자리)

    @JsonProperty("ACNT_PRDT_CD")
    private String acntPrdtCd;   // 계좌상품코드(2자리)

    @JsonProperty("PDNO")
    private String pdno;         // 종목코드(6자리)

    @JsonProperty("ORD_DVSN")
    private String ordDvsn;      // 주문구분 (00: 지정가, 01: 시장가)

    @JsonProperty("ORD_QTY")
    private String ordQty;       // 주문수량

    @JsonProperty("ORD_UNPR")
    private String ordUnpr;      // 주문단가 (시장가일 경우 "0")
}