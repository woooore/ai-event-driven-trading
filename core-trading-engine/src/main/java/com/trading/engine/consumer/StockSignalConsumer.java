package com.trading.engine.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.engine.dto.StockSignalDto;
import com.trading.engine.service.KisOrderService;
import com.trading.engine.service.PortfolioManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class StockSignalConsumer {

    private final ObjectMapper objectMapper;
    private final KisOrderService kisOrderService;
    private final PortfolioManager portfolioManager;
    private final RedissonClient redissonClient;

    private static final long COOLDOWN_SECONDS = 60;

    @KafkaListener(topics = "stock-signals", groupId = "trading-engine-group")
    public void consumeSignal(String message) {
        try {
            StockSignalDto signal = objectMapper.readValue(message, StockSignalDto.class);

            String ticker = signal.getTicker();
            if (ticker == null || ticker.isBlank()) {
                log.warn(">> 유효하지 않은 티커의 시그널 수신: {}", message);
                return;
            }
            ticker = ticker.toUpperCase().trim();

            // 1. HOLD 시그널인 경우 관망 처리
            if ("HOLD".equalsIgnoreCase(signal.getAction())) {
                log.info(">> [AI 시그널 분석] {} - 관망(HOLD) 유지. 사유: {}", ticker, signal.getReason());
                return;
            }

            // 2. SELL (악재 감지) 시그널인 경우: 보유 중이면 즉시 긴급 매도!
            if ("SELL".equalsIgnoreCase(signal.getAction())) {
                if (!portfolioManager.hasPosition(ticker)) {
                    log.info(">> [AI 매도 시그널 수신] {} 악재 감지되었으나 미보유 종목이므로 스킵. 사유: {}", ticker, signal.getReason());
                    return;
                }

                RLock sellLock = redissonClient.getLock("lock:order:" + ticker);
                boolean acquired = sellLock.tryLock(2, 10, TimeUnit.SECONDS);
                if (!acquired) {
                    log.warn(">> [Redis 분산 락] {} 매도 주문 처리 중 다른 스레드가 점유 중입니다. 스킵.", ticker);
                    return;
                }
                try {
                    log.warn(">> 🚨 [AI 악재 감지 긴급 매도 실행] 보유 종목 {}에 치명적 악재 감지! 전량 매도 실행. 사유: {}",
                            ticker, signal.getReason());
                    kisOrderService.executeOverseasOrder(signal);
                } finally {
                    if (sellLock.isHeldByCurrentThread()) {
                        sellLock.unlock();
                    }
                }
                return;
            }

            // 3. BUY 시그널: Redis 분산 락 및 퀀트 기술적 지표 필터링
            RLock buyLock = redissonClient.getLock("lock:order:" + ticker);
            boolean acquired = buyLock.tryLock(2, 15, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn(">> [Redis 분산 락 획득 실패] {} 종목 주문이 이미 진행 중입니다. (스킵)", ticker);
                return;
            }

            try {
                // (1) Redis 분산 쿨다운 검사
                RBucket<String> cooldownBucket = redissonClient.getBucket("cooldown:order:" + ticker);
                if (cooldownBucket.isExists()) {
                    log.warn(">> ⏱️ [Redis 분산 쿨다운 스킵] {} 종목은 최근 주문 후 쿨다운(60초) 중입니다.", ticker);
                    return;
                }

                // (2) 퀀트 기술적 지표 2차 안전장치: RSI >= 70 (과매수 단기 고점 필터링)
                if (signal.getRsi() != null && signal.getRsi() >= 70.0) {
                    log.warn(">> 🛡️ [퀀트 기술적 필터 차단] {} 매수 스킵: RSI({} >= 70.0) 단기 과매수 과열! 세력 차익실현 물량받이 위험 차단. 사유: {}",
                            ticker, String.format("%.1f", signal.getRsi()), signal.getReason());
                    return;
                }

                // (3) 이동평균선 이격도 2차 안전장치: 이격도 >= 110% (단기 급등 과열)
                if (signal.getMaDisparity() != null && signal.getMaDisparity() >= 110.0) {
                    log.warn(">> 🛡️ [퀀트 기술적 필터 차단] {} 매수 스킵: 20일 이격도({}% >= 110%) 급등 과열! 사유: {}",
                            ticker, String.format("%.1f", signal.getMaDisparity()), signal.getReason());
                    return;
                }

                // (4) 해외주식 매수 주문 실행
                kisOrderService.executeOverseasOrder(signal);

                // (5) Redis 분산 쿨다운 설정 (60초 TTL)
                cooldownBucket.set("ORDERED", Duration.ofSeconds(COOLDOWN_SECONDS));
                log.info(">> 🔒 [Redis 분산 락 & 쿨다운 등록] {} 종목 주문 체결 및 60초 쿨다운 적용 완료.", ticker);

            } finally {
                if (buyLock.isHeldByCurrentThread()) {
                    buyLock.unlock();
                }
            }

        } catch (JsonProcessingException e) {
            log.error("Kafka 메시지 파싱 오류: {}", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Redis 분산 락 대기 중 인터럽트 발생: {}", e.getMessage());
        } catch (Exception e) {
            log.error("시그널 처리 중 예기치 않은 오류 발생: {}", e.getMessage(), e);
        }
    }
}