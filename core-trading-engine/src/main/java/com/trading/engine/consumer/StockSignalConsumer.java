package com.trading.engine.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.engine.dto.StockSignalDto;
import com.trading.engine.service.KisOrderService;
import com.trading.engine.service.PortfolioManager;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class StockSignalConsumer {

    private final ObjectMapper objectMapper;
    private final KisOrderService kisOrderService;
    private final PortfolioManager portfolioManager;
    private final RedissonClient redissonClient;
    private final com.trading.engine.service.AnalysisLogService analysisLogService;
    private final com.trading.engine.service.MarketBriefingService marketBriefingService;
    private final com.trading.engine.service.EarningsService earningsService;

    // Redisson 연결 불가 시 안전한 인메모리 쿨다운 폴백
    private final ConcurrentHashMap<String, Instant> inMemoryCooldown = new ConcurrentHashMap<>();
    private static final long COOLDOWN_SECONDS = 60;

    public StockSignalConsumer(
            ObjectMapper objectMapper,
            KisOrderService kisOrderService,
            PortfolioManager portfolioManager,
            com.trading.engine.service.AnalysisLogService analysisLogService,
            com.trading.engine.service.MarketBriefingService marketBriefingService,
            com.trading.engine.service.EarningsService earningsService,
            @Autowired(required = false) RedissonClient redissonClient) {
        this.objectMapper = objectMapper;
        this.kisOrderService = kisOrderService;
        this.portfolioManager = portfolioManager;
        this.analysisLogService = analysisLogService;
        this.marketBriefingService = marketBriefingService;
        this.earningsService = earningsService;
        this.redissonClient = redissonClient;
    }

    /**
     * AI 워커의 실시간 종목 감지/분석 로그 수신
     */
    @KafkaListener(topics = "stock-analysis-logs", groupId = "trading-engine-log-group")
    public void consumeAnalysisLog(String message) {
        try {
            com.trading.engine.dto.AnalysisLogDto dto = objectMapper.readValue(message, com.trading.engine.dto.AnalysisLogDto.class);
            analysisLogService.addLog(dto);
        } catch (Exception e) {
            log.error("분석 로그 Kafka 수신 오류: {}", e.getMessage());
        }
    }

    /**
     * AI 워커의 1시간 단위 시장 브리핑 수신
     */
    @KafkaListener(topics = "market-briefings", groupId = "trading-engine-briefing-group")
    public void consumeMarketBriefing(String message) {
        try {
            com.trading.engine.dto.MarketBriefingDto dto = objectMapper.readValue(message, com.trading.engine.dto.MarketBriefingDto.class);
            marketBriefingService.addBriefing(dto);
        } catch (Exception e) {
            log.error("시장 브리핑 Kafka 수신 오류: {}", e.getMessage());
        }
    }

    /**
     * 미국 시총 상위 100대 기업 실적 발표 및 어닝스 이벤트 수신
     */
    @KafkaListener(topics = "earnings-events", groupId = "trading-engine-earnings-group")
    public void consumeEarningsEvent(String message) {
        try {
            com.trading.engine.dto.EarningsEventDto dto = objectMapper.readValue(message, com.trading.engine.dto.EarningsEventDto.class);
            earningsService.addEarningsEvent(dto);
        } catch (Exception e) {
            log.error("실적 이벤트 Kafka 수신 오류: {}", e.getMessage());
        }
    }

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

                if (redissonClient != null) {
                    RLock sellLock = redissonClient.getLock("lock:order:" + ticker);
                    boolean acquired = sellLock.tryLock(2, 10, TimeUnit.SECONDS);
                    if (!acquired) {
                        log.warn(">> [Redis 분산 락] {} 매도 주문 처리 중 다른 스레드가 점유 중입니다. 스킵.", ticker);
                        return;
                    }
                    try {
                        log.warn(">> 🚨 [AI 악재 긴급 매도] {} 보유 전량 매도 실행! 사유: {}", ticker, signal.getReason());
                        kisOrderService.executeOverseasOrder(signal);
                    } finally {
                        if (sellLock.isHeldByCurrentThread()) {
                            sellLock.unlock();
                        }
                    }
                } else {
                    log.warn(">> 🚨 [AI 악재 긴급 매도 (인메모리 모드)] {} 보유 전량 매도 실행! 사유: {}", ticker, signal.getReason());
                    kisOrderService.executeOverseasOrder(signal);
                }
                return;
            }

            // 3. BUY 시그널: 퀀트 기술적 지표 필터링 (극단적 천장 과열만 차단하고 70~75.9 모멘텀은 허용)
            if (signal.getRsi() != null && signal.getRsi() >= 76.0) {
                log.warn(">> 🛡️ [퀀트 기술적 필터 차단] {} 매수 스킵: RSI({} >= 76.0) 극단적 단기 과매수 과열! 세력 차익실현 물량받이 위험 차단. 사유: {}",
                        ticker, String.format("%.1f", signal.getRsi()), signal.getReason());
                return;
            }

            if (signal.getMaDisparity() != null && signal.getMaDisparity() >= 115.0) {
                log.warn(">> 🛡️ [퀀트 기술적 필터 차단] {} 매수 스킵: 20일 이격도({}% >= 115%) 급등 과열! 사유: {}",
                        ticker, String.format("%.1f", signal.getMaDisparity()), signal.getReason());
                return;
            }

            // 4. Redis 분산 락 또는 인메모리 락 기반 매수 주문 실행
            if (redissonClient != null) {
                RLock buyLock = redissonClient.getLock("lock:order:" + ticker);
                boolean acquired = buyLock.tryLock(2, 15, TimeUnit.SECONDS);
                if (!acquired) {
                    log.warn(">> [Redis 분산 락 획득 실패] {} 종목 주문이 이미 진행 중입니다. (스킵)", ticker);
                    return;
                }

                try {
                    RBucket<String> cooldownBucket = redissonClient.getBucket("cooldown:order:" + ticker);
                    if (cooldownBucket.isExists()) {
                        log.warn(">> ⏱️ [Redis 분산 쿨다운 스킵] {} 종목은 최근 주문 후 쿨다운(60초) 중입니다.", ticker);
                        return;
                    }

                    kisOrderService.executeOverseasOrder(signal);
                    cooldownBucket.set("ORDERED", Duration.ofSeconds(COOLDOWN_SECONDS));
                    log.info(">> 🔒 [Redis 분산 락 & 쿨다운 등록] {} 종목 주문 체결 및 60초 쿨다운 적용 완료.", ticker);
                } finally {
                    if (buyLock.isHeldByCurrentThread()) {
                        buyLock.unlock();
                    }
                }
            } else {
                // 인메모리 폴백 모드
                Instant now = Instant.now();
                Instant lastTime = inMemoryCooldown.get(ticker);
                if (lastTime != null && now.isBefore(lastTime.plusSeconds(COOLDOWN_SECONDS))) {
                    log.warn(">> ⏱️ [인메모리 쿨다운 스킵] {} 종목 최근 주문 후 쿨다운 중입니다.", ticker);
                    return;
                }

                kisOrderService.executeOverseasOrder(signal);
                inMemoryCooldown.put(ticker, now);
                log.info(">> 🔒 [인메모리 쿨다운 등록] {} 종목 주문 체결 및 60초 쿨다운 적용 완료.", ticker);
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