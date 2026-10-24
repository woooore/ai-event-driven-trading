package com.trading.engine.controller;

import com.trading.engine.dto.AnalysisLogDto;
import com.trading.engine.dto.MarketBriefingDto;
import com.trading.engine.service.AnalysisLogService;
import com.trading.engine.service.MarketBriefingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class BriefingRestController {

    private final MarketBriefingService marketBriefingService;
    private final AnalysisLogService analysisLogService;

    /**
     * 1시간 단위 AI 브리핑 조회 (날짜별)
     */
    @GetMapping("/briefings")
    public ResponseEntity<List<MarketBriefingDto>> getBriefings(@RequestParam(required = false) String date) {
        return ResponseEntity.ok(marketBriefingService.getBriefingsByDate(date));
    }

    /**
     * AI 브리핑 가용 날짜 목록 조회
     */
    @GetMapping("/briefings/dates")
    public ResponseEntity<List<String>> getBriefingDates() {
        return ResponseEntity.ok(marketBriefingService.getAvailableDates());
    }

    /**
     * AI 브리핑 수동 또는 워커 등록
     */
    @PostMapping("/briefings")
    public ResponseEntity<Map<String, Object>> addBriefing(@RequestBody MarketBriefingDto dto) {
        marketBriefingService.addBriefing(dto);
        return ResponseEntity.ok(Map.of("status", "SUCCESS", "message", "브리핑이 등록되었습니다."));
    }

    /**
     * 종목별 실시간 AI 감지/판단 로그 조회 (날짜별)
     */
    @GetMapping("/analysis-logs")
    public ResponseEntity<List<AnalysisLogDto>> getAnalysisLogs(@RequestParam(required = false) String date) {
        return ResponseEntity.ok(analysisLogService.getLogsByDate(date));
    }

    /**
     * 분석 로그 가용 날짜 목록 조회
     */
    @GetMapping("/analysis-logs/dates")
    public ResponseEntity<List<String>> getAnalysisLogDates() {
        return ResponseEntity.ok(analysisLogService.getAvailableDates());
    }

    /**
     * 분석 로그 등록 (HTTP 방식)
     */
    @PostMapping("/analysis-logs")
    public ResponseEntity<Map<String, Object>> addAnalysisLog(@RequestBody AnalysisLogDto dto) {
        analysisLogService.addLog(dto);
        return ResponseEntity.ok(Map.of("status", "SUCCESS"));
    }
}
