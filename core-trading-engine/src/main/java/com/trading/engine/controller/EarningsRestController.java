package com.trading.engine.controller;

import com.trading.engine.dto.EarningsEventDto;
import com.trading.engine.dto.UpcomingEarningsDto;
import com.trading.engine.service.EarningsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/earnings")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class EarningsRestController {

    private final EarningsService earningsService;

    /**
     * 미국 시총 상위 100대 기업 실적 발표 및 가이던스 이벤트 목록 조회
     */
    @GetMapping
    public ResponseEntity<List<EarningsEventDto>> getEarnings(
            @RequestParam(required = false) String filter,
            @RequestParam(required = false) String date) {
        return ResponseEntity.ok(earningsService.getEarnings(filter, date));
    }

    /**
     * 실적 통계 요약 (총 건수, 서프라이즈, 가이던스 상향, 쇼크)
     */
    @GetMapping("/summary")
    public ResponseEntity<Map<String, Object>> getSummary() {
        return ResponseEntity.ok(earningsService.getSummary());
    }

    /**
     * ⏰ 향후 2주 이내 실적 발표 예정 리스트 (D-Day 순)
     */
    @GetMapping("/upcoming")
    public ResponseEntity<List<UpcomingEarningsDto>> getUpcomingEarnings() {
        return ResponseEntity.ok(earningsService.getUpcomingEarnings());
    }

    /**
     * 실적 이벤트 등록 (워커 또는 수동)
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> addEarningsEvent(@RequestBody EarningsEventDto dto) {
        earningsService.addEarningsEvent(dto);
        return ResponseEntity.ok(Map.of("status", "SUCCESS", "message", "실적 이벤트가 등록되었습니다."));
    }
}
