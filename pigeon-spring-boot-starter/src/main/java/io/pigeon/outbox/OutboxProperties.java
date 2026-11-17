package io.pigeon.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "pigeon.outbox")
public record OutboxProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("50") int batchSize,
        @DefaultValue("500") long pollIntervalMs,
        @DefaultValue("8") int maxAttempts,
        @DefaultValue("1000") long baseBackoffMs,
        @DefaultValue("300000") long maxBackoffMs,
        @DefaultValue("5000") long confirmTimeoutMs,
        /** Delay before retrying when the broker is down (does not increment attempts). */
        @DefaultValue("5000") long brokerDownBackoffMs,
        @DefaultValue("604800000") long sentRetentionMs,
        @DefaultValue("3600000") long sentCleanupIntervalMs,
        @DefaultValue("pigeon.events") String exchange,
        @DefaultValue("pigeon.dlx") String deadLetterExchange,
        @DefaultValue("pigeon.dlq") String deadLetterQueue) {

    public static OutboxProperties defaults() {
        return new OutboxProperties(true, 50, 500, 8, 1000, 300_000, 5000, 5000,
                604_800_000, 3_600_000,
                "pigeon.events", "pigeon.dlx", "pigeon.dlq");
    }
}
