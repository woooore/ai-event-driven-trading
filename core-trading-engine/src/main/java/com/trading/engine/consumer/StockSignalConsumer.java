package com.trading.engine.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.engine.dto.StockSignalDto;
import com.trading.engine.service.KisOrderService;
import com.trading.engine.service.PortfolioManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class StockSignalConsumer {

    private final ObjectMapper objectMapper;
    private final KisOrderService kisOrderService;
    private final PortfolioManager portfolioManager;

    // 중복 주문 방지용 캐시 (티커 -> 마지막 주문 타임스탬프)
    private final ConcurrentHashMap<String, Instant> orderCooldownCache = new ConcurrentHashMap<>();
    private static final long COOLDOWN_SECONDS = 30;

    @KafkaListener(topics = "stock-signals", groupId = "trading-engine-group")
    public void consumeSignal(String message) {
        try {
            StockSignalDto signal = objectMapper.readValue(message, StockSignalDto.class);

            String ticker = signal.getTicker();
            if (ticker == null || ticker.isBlank()) {
                log.warn(">> 유효하지 않은 티커의 시그널 수신: {}", message);
                return;
            }

            // 1. HOLD 시그널인 경우 관망 처리
            if ("HOLD".equalsIgnoreCase(signal.getAction())) {
                log.info(">> [AI 시그널 분석] {} - 관망(HOLD) 유지. 사유: {}", ticker, signal.getReason());
                return;
            }

            // 2. SELL (악재 감지) 시그널인 경우: 보유 중이면 즉시 긴급 매도!
            if ("SELL".equalsIgnoreCase(signal.getAction())) {
                if (portfolioManager.hasPosition(ticker)) {
                    log.warn(">> 🚨 [AI 악재 감지 긴급 매도 실행] 보유 종목 {}에 치명적 악재 감지! 전량 매도 실행. 사유: {}",
                            ticker, signal.getReason());
                    kisOrderService.executeOverseasOrder(signal);
                } else {
                    log.info(">> [AI 매도 시그널 수신] {} 악재 감지되었으나 미보유 종목이므로 스킵. 사유: {}", ticker, signal.getReason());
                }
                return;
            }

            // 3. BUY 시그널: 중복 주문 방지 (쿨다운 30초)
            Instant now = Instant.now();
            Instant lastOrderTime = orderCooldownCache.get(ticker);
            if (lastOrderTime != null && now.isBefore(lastOrderTime.plusSeconds(COOLDOWN_SECONDS))) {
                log.warn(">> [중복 방지] {} 종목은 최근 주문 후 쿨다운 중입니다. (스킵 처리)", ticker);
                return;
            }

            // 4. 해외주식 매수 주문 실행
            kisOrderService.executeOverseasOrder(signal);

            // 쿨다운 갱신
            orderCooldownCache.put(ticker, now);

        } catch (JsonProcessingException e) {
            log.error("Kafka 메시지 파싱 오류: {}", e.getMessage());
        } catch (Exception e) {
            log.error("시그널 처리 중 예기치 않은 오류 발생: {}", e.getMessage(), e);
        }
    }
}