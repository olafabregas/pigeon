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
        /** Fraction (0-1) by which each retry delay is randomly shortened, so retries do not synchronize. */
        @DefaultValue("0.2") double backoffJitter,
        @DefaultValue("5000") long confirmTimeoutMs,
        /** Delay before retrying when the broker is down (does not increment attempts). */
        @DefaultValue("5000") long brokerDownBackoffMs,
        @DefaultValue("604800000") long sentRetentionMs,
        @DefaultValue("3600000") long sentCleanupIntervalMs,
        @DefaultValue("60000") long claimLeaseMs,
        @DefaultValue Flyway flyway,
        @DefaultValue("pigeon.events") String exchange,
        @DefaultValue("pigeon.dlx") String deadLetterExchange,
        @DefaultValue("pigeon.dlq") String deadLetterQueue) {

    /** Binds {@code pigeon.outbox.flyway.*}. */
    public record Flyway(
            @DefaultValue("false") boolean enabled,
            @DefaultValue("classpath:db/pigeon") String locations,
            @DefaultValue("pigeon_schema_history") String table) {
    }

    public static OutboxProperties defaults() {
        return new OutboxProperties(true, 50, 500, 8, 1000, 300_000, 0.2, 5000, 5000,
                604_800_000, 3_600_000,
                60_000,
                new Flyway(false, "classpath:db/pigeon", "pigeon_schema_history"),
                "pigeon.events", "pigeon.dlx", "pigeon.dlq");
    }
}
