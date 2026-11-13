package io.pigeon.gateway.ratelimit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
public class RateLimitFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);

    private final RateLimitProperties props;
    private final TokenBucketLimiter limiter;
    private final Counter rejected;
    private final AntPathMatcher matcher = new AntPathMatcher();

    public RateLimitFilter(RateLimitProperties props, ReactiveStringRedisTemplate redis, MeterRegistry registry) {
        this.props = props;
        this.limiter = new TokenBucketLimiter(redis, props.getRps(), props.getBurst());
        this.rejected = Counter.builder("pigeon.gateway.rate_limit.rejected")
                .description("Requests rejected by the gateway rate limiter").register(registry);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 10; // after correlation id filter
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        if (!props.isEnabled() || !matcher.match(props.getPathPattern(), exchange.getRequest().getPath().value())) {
            return chain.filter(exchange);
        }
        String ip = clientIp(exchange);
        return limiter.tryAcquire(ip)
                .onErrorResume(e -> {
                    log.warn("Rate limiter unavailable, failing open: {}", e.toString());
                    return Mono.empty();
                })
                .map(java.util.Optional::of)
                .defaultIfEmpty(java.util.Optional.empty())
                .flatMap(opt -> {
                    if (opt.isEmpty()) return chain.filter(exchange);
                    TokenBucketLimiter.Result r = opt.get();
                    HttpHeaders h = exchange.getResponse().getHeaders();
                    h.set("X-RateLimit-Limit", String.valueOf(props.getBurst()));
                    h.set("X-RateLimit-Remaining", String.valueOf(r.remaining()));
                    if (r.allowed()) return chain.filter(exchange);
                    rejected.increment();
                    long retrySec = Math.max(1, (r.retryAfterMs() + 999) / 1000);
                    exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
                    h.set(HttpHeaders.RETRY_AFTER, String.valueOf(retrySec));
                    h.setContentType(MediaType.APPLICATION_JSON);
                    byte[] body = ("{\"error\":\"rate_limited\",\"retryAfterSeconds\":" + retrySec + "}")
                            .getBytes(StandardCharsets.UTF_8);
                    return exchange.getResponse().writeWith(
                            Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
                });
    }

    static String clientIp(ServerWebExchange exchange) {
        String xff = exchange.getRequest().getHeaders().getFirst("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) return xff.split(",")[0].trim();
        InetSocketAddress addr = exchange.getRequest().getRemoteAddress();
        return addr != null && addr.getAddress() != null ? addr.getAddress().getHostAddress() : "unknown";
    }
}
