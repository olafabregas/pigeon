package io.pigeon.gateway;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
public class CorrelationIdFilter implements GlobalFilter, Ordered {

    public static final String HEADER = "X-Correlation-Id";
    private static final Logger log = LoggerFactory.getLogger(CorrelationIdFilter.class);

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String id = exchange.getRequest().getHeaders().getFirst(HEADER);
        if (id == null || id.isBlank()) id = UUID.randomUUID().toString();
        final String cid = id;
        ServerHttpRequest req = exchange.getRequest().mutate().header(HEADER, cid).build();
        exchange.getResponse().getHeaders().set(HEADER, cid);
        return chain.filter(exchange.mutate().request(req).build())
                .doFirst(() -> MDC.put("correlationId", cid))
                .doFinally(s -> {
                    MDC.put("correlationId", cid);
                    log.info("{} {} -> {}", req.getMethod(), req.getURI().getPath(),
                            exchange.getResponse().getStatusCode());
                    MDC.remove("correlationId");
                });
    }
}
