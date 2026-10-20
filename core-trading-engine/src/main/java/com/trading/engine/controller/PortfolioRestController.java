package com.trading.engine.controller;

import com.trading.engine.service.PortfolioManager;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/portfolio")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class PortfolioRestController {

    private final PortfolioManager portfolioManager;

    /**
     * 포트폴리오 실시간 현황 및 거래 기록 조회
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getPortfolio() {
        return ResponseEntity.ok(portfolioManager.getDashboardData());
    }

    /**
     * 특정 보유 종목 수동 매도 (테스트 및 긴급 탈출용)
     */
    @PostMapping("/sell/{ticker}")
    public ResponseEntity<Map<String, Object>> manualSell(@PathVariable String ticker) {
        var pos = portfolioManager.getPosition(ticker);
        if (pos == null) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", "보유하고 있지 않은 종목입니다."));
        }
        boolean success = portfolioManager.sellPosition(ticker, pos.getCurrentPrice(), "사용자 웹 수동 매도");
        return ResponseEntity.ok(Map.of("success", success, "message", ticker + " 매도 처리 완료"));
    }
}
