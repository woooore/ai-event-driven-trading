package com.trading.engine.config;

import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class RedissonConfig {

    @Value("${REDIS_HOST:${spring.data.redis.host:127.0.0.1}}")
    private String redisHost;

    @Value("${REDIS_PORT:${spring.data.redis.port:6379}}")
    private int redisPort;

    @Bean
    public RedissonClient redissonClient() {
        String address = "redis://" + redisHost + ":" + redisPort;
        log.info(">> [Redisson 초기화] Redis 연결 주소: {}", address);
        try {
            Config config = new Config();
            config.useSingleServer()
                    .setAddress(address)
                    .setConnectionMinimumIdleSize(1)
                    .setConnectionPoolSize(4)
                    .setConnectTimeout(10000)
                    .setTimeout(3000)
                    .setRetryAttempts(5)
                    .setRetryInterval(1500);
            RedissonClient client = Redisson.create(config);
            log.info(">> [Redisson] Redis({}) 분산 락 클러스터 연결 성공!", address);
            return client;
        } catch (Exception e) {
            log.warn(">> [Redisson 연결 대기/실패]: {}. 인메모리 락 모드로 안전하게 자동 폴백합니다.", e.getMessage());
            return null;
        }
    }
}

