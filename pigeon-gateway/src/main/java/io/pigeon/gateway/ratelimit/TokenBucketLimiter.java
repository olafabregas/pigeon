package io.pigeon.gateway.ratelimit;

import java.util.List;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Mono;

/** Atomic Redis token bucket (see scripts/token_bucket.lua). */
public class TokenBucketLimiter {

    public record Result(boolean allowed, long remaining, long retryAfterMs) {}

    private static final RedisScript<List> SCRIPT = loadScript();

    private final ReactiveStringRedisTemplate redis;
    private final double rps;
    private final int burst;

    public TokenBucketLimiter(ReactiveStringRedisTemplate redis, double rps, int burst) {
        this.redis = redis;
        this.rps = rps;
        this.burst = burst;
    }

    @SuppressWarnings("unchecked")
    private static RedisScript<List> loadScript() {
        return RedisScript.of(new ClassPathResource("scripts/token_bucket.lua"), List.class);
    }

    public int burst() { return burst; }

    @SuppressWarnings("unchecked")
    public Mono<Result> tryAcquire(String key) {
        return redis.execute(SCRIPT, List.of("pigeon:rl:" + key),
                        List.of(String.valueOf(rps), String.valueOf(burst), "1"))
                .next()
                .map(raw -> {
                    List<Object> r = (List<Object>) raw;
                    return new Result(((Number) r.get(0)).longValue() == 1L,
                            ((Number) r.get(1)).longValue(),
                            ((Number) r.get(2)).longValue());
                });
    }
}
