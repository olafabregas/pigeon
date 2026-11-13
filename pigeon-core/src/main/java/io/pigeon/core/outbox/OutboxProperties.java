package io.pigeon.core.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pigeon.outbox")
public record OutboxProperties(
        boolean enabled,
        int batchSize,
        long pollIntervalMs,
        int maxAttempts,
        long baseBackoffMs,
        long maxBackoffMs,
        long confirmTimeoutMs) {

    public static OutboxProperties defaults() {
        return new OutboxProperties(true, 50, 500, 8, 1000, 300_000, 5000);
    }
}
