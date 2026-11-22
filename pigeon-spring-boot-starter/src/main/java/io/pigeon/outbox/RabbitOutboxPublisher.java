package io.pigeon.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class RabbitOutboxPublisher implements OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(RabbitOutboxPublisher.class);
    public static final String MESSAGE_ID_HEADER = "messageId";
    private static final long RETURN_GRACE_MS = 50;

    private final RabbitTemplate template;
    private final OutboxProperties props;
    private final AtomicBoolean resetSuggested = new AtomicBoolean();

    public RabbitOutboxPublisher(RabbitTemplate template, OutboxProperties props) {
        this.template = template;
        this.props = props;
    }

    @Override
    public PublishBatchResult publish(List<OutboxEvent> events) {
        if (events.isEmpty()) {
            return PublishBatchResult.of(Map.of());
        }
        if (resetSuggested.compareAndSet(true, false)) {
            resetConnection("previous transient confirm failure");
        }
        if (!brokerReachable()) {
            log.warn("broker unreachable; deferring {} outbox event(s) without burning attempts", events.size());
            resetSuggested.set(true);
            return PublishBatchResult.all(events, PublishOutcome.TRANSIENT, "broker unreachable");
        }

        Map<UUID, CorrelationData> pending = new HashMap<>();
        Map<UUID, PublishOutcome> outcomes = new HashMap<>();
        Map<UUID, String> errors = new HashMap<>();
        boolean resetAfterSendFailure = false;
        for (OutboxEvent e : events) {
            try {
                CorrelationData cd = new CorrelationData(e.id().toString());
                pending.put(e.id(), cd);
                template.send(props.exchange(), e.eventType(), message(e), cd);
            } catch (RuntimeException ex) {
                pending.remove(e.id());
                PublishOutcome outcome = isTransient(ex)
                        ? PublishOutcome.TRANSIENT
                        : PublishOutcome.PERMANENT;
                outcomes.put(e.id(), outcome);
                errors.put(e.id(), "send failed: " + ex);
                log.warn("publish send failed for {} ({}): {}", e.id(), outcome, ex.toString());
                resetAfterSendFailure |= outcome == PublishOutcome.TRANSIENT;
            }
        }

        boolean sawConfirmTimeout = false;
        for (Map.Entry<UUID, CorrelationData> en : pending.entrySet()) {
            if (sawConfirmTimeout) {
                // Don't burn N × confirmTimeout waiting on a dead confirm channel.
                outcomes.put(en.getKey(), PublishOutcome.TRANSIENT);
                errors.put(en.getKey(), "not awaited: an earlier confirm in this batch timed out");
                continue;
            }
            // Per-event timeout — do not share one deadline across the whole batch
            // (that made later events false-timeout and leave SENT stuck after outages).
            Resolution resolution = resolveConfirm(en.getKey(), en.getValue(), props.confirmTimeoutMs());
            PublishOutcome outcome = resolution.outcome();
            outcomes.put(en.getKey(), outcome);
            if (resolution.error() != null) {
                errors.put(en.getKey(), resolution.error());
            }
            if (outcome == PublishOutcome.TRANSIENT) {
                sawConfirmTimeout = true;
            }
        }
        if (sawConfirmTimeout || resetAfterSendFailure) {
            // Stale channels after broker restart often deliver messages but never complete confirms.
            resetSuggested.set(true);
            resetConnection(sawConfirmTimeout ? "confirm timeout" : "send failed");
        }
        return PublishBatchResult.of(outcomes, errors);
    }

    private record Resolution(PublishOutcome outcome, String error) {
        static Resolution confirmed() {
            return new Resolution(PublishOutcome.CONFIRMED, null);
        }

        static Resolution transientFailure(String error) {
            return new Resolution(PublishOutcome.TRANSIENT, error);
        }

        static Resolution permanentFailure(String error) {
            return new Resolution(PublishOutcome.PERMANENT, error);
        }
    }

    private Resolution resolveConfirm(UUID id, CorrelationData cd, long timeoutMs) {
        try {
            CorrelationData.Confirm c = cd.getFuture().get(timeoutMs, TimeUnit.MILLISECONDS);
            if (c.isAck()) {
                long remaining = RETURN_GRACE_MS;
                while (cd.getReturned() == null && remaining > 0) {
                    long started = System.nanoTime();
                    try {
                        Thread.sleep(Math.min(5, remaining));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    remaining -= (System.nanoTime() - started) / 1_000_000;
                }
                if (cd.getReturned() == null) {
                    return Resolution.confirmed();
                }
            }
            log.warn("event {} not confirmed: ack={} returned={} reason={}",
                    id, c.isAck(), cd.getReturned() != null, c.getReason());
            String error = cd.getReturned() != null
                    ? "unroutable: message returned by broker (" + cd.getReturned().getReplyText() + ")"
                    : "broker nacked the publish" + (c.getReason() == null ? "" : ": " + c.getReason());
            return Resolution.permanentFailure(error);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return Resolution.transientFailure("interrupted while waiting for broker confirm");
        } catch (TimeoutException te) {
            log.warn("event {} confirm timed out (transient)", id);
            return Resolution.transientFailure("broker confirm timed out after " + timeoutMs + " ms");
        } catch (Exception ex) {
            if (isTransient(ex)) {
                log.warn("event {} confirm failed (transient): {}", id, ex.toString());
                return Resolution.transientFailure("confirm failed: " + ex);
            }
            log.warn("event {} confirm failed (permanent): {}", id, ex.toString());
            return Resolution.permanentFailure("confirm failed: " + ex);
        }
    }

    private boolean brokerReachable() {
        ConnectionFactory cf = template.getConnectionFactory();
        try (var ignored = cf.createConnection()) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void resetConnection(String reason) {
        ConnectionFactory cf = template.getConnectionFactory();
        if (cf instanceof CachingConnectionFactory caching) {
            log.warn("resetting RabbitMQ cached connection ({})", reason);
            caching.resetConnection();
        }
    }

    private static boolean isTransient(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof AmqpConnectException
                    || t instanceof AmqpIOException
                    || t instanceof AmqpTimeoutException
                    || t instanceof TimeoutException
                    || t instanceof java.net.ConnectException
                    || t instanceof java.net.SocketException) {
                return true;
            }
            String msg = t.getMessage();
            if (msg != null) {
                String m = msg.toLowerCase();
                if (m.contains("connection refused")
                        || m.contains("connection reset")
                        || m.contains("broken pipe")
                        || m.contains("not available")
                        || m.contains("channel shutdown")) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void deadLetter(OutboxEvent event) {
        try {
            template.send(props.deadLetterExchange(), "dead." + event.eventType(), message(event));
        } catch (AmqpException e) {
            log.warn("dead-letter publish failed for {}: {}", event.id(), e.toString());
        }
    }

    private Message message(OutboxEvent e) {
        return MessageBuilder.withBody(e.payload().getBytes(StandardCharsets.UTF_8))
                .setContentType("application/json")
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .setHeader(MESSAGE_ID_HEADER, e.id().toString())
                .setMessageId(e.id().toString())
                .setType(e.eventType())
                .build();
    }
}
