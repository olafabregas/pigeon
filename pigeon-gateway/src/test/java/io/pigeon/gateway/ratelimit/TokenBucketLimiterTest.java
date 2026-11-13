package io.pigeon.gateway.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Testcontainers(disabledWithoutDocker = true)
class TokenBucketLimiterTest {

    static GenericContainer<?> redis;
    static LettuceConnectionFactory cf;
    static ReactiveStringRedisTemplate template;

    @BeforeAll
    static void start() {
        redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
        redis.start();
        cf = new LettuceConnectionFactory(new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379)));
        cf.afterPropertiesSet();
        template = new ReactiveStringRedisTemplate(cf);
    }

    @AfterAll
    static void stop() {
        if (cf != null) cf.destroy();
        if (redis != null) redis.stop();
    }

    @Test
    void allowsBurstThenRejectsWithRetryAfter() {
        TokenBucketLimiter limiter = new TokenBucketLimiter(template, 0.001, 5);
        String key = UUID.randomUUID().toString();
        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire(key).block().allowed()).isTrue();
        }
        TokenBucketLimiter.Result r = limiter.tryAcquire(key).block();
        assertThat(r.allowed()).isFalse();
        assertThat(r.retryAfterMs()).isPositive();
    }

    @Test
    void refillsOverTime() throws Exception {
        TokenBucketLimiter limiter = new TokenBucketLimiter(template, 20, 1);
        String key = UUID.randomUUID().toString();
        assertThat(limiter.tryAcquire(key).block().allowed()).isTrue();
        assertThat(limiter.tryAcquire(key).block().allowed()).isFalse();
        Thread.sleep(150);
        assertThat(limiter.tryAcquire(key).block().allowed()).isTrue();
    }

    @Test
    void concurrentRequestsNeverExceedBurst() {
        int burst = 20;
        // negligible refill so exactly `burst` may pass
        TokenBucketLimiter limiter = new TokenBucketLimiter(template, 0.001, burst);
        String key = UUID.randomUUID().toString();
        AtomicInteger allowed = new AtomicInteger();
        List<Boolean> results = Flux.range(0, 200)
                .flatMap(i -> limiter.tryAcquire(key).subscribeOn(Schedulers.parallel()), 50)
                .map(TokenBucketLimiter.Result::allowed)
                .collectList().block();
        results.forEach(a -> { if (a) allowed.incrementAndGet(); });
        assertThat(results).hasSize(200);
        assertThat(allowed.get()).isEqualTo(burst);
    }
}
