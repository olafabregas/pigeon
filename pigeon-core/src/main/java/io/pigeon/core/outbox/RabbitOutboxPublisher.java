package io.pigeon.core.outbox;

import io.pigeon.core.config.RabbitConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class RabbitOutboxPublisher implements OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(RabbitOutboxPublisher.class);
    public static final String MESSAGE_ID_HEADER = "messageId";

    private final RabbitTemplate template;
    private final OutboxProperties props;

    public RabbitOutboxPublisher(RabbitTemplate template, OutboxProperties props) {
        this.template = template;
        this.props = props;
    }

    @Override
    public Set<UUID> publish(List<OutboxEvent> events) {
        Map<UUID, CorrelationData> pending = new HashMap<>();
        for (OutboxEvent e : events) {
            CorrelationData cd = new CorrelationData(e.id().toString());
            pending.put(e.id(), cd);
            template.send(RabbitConfig.EXCHANGE, e.eventType(), message(e), cd);
        }
        Set<UUID> ok = new HashSet<>();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(props.confirmTimeoutMs());
        for (Map.Entry<UUID, CorrelationData> en : pending.entrySet()) {
            try {
                long remaining = Math.max(1, deadline - System.nanoTime());
                CorrelationData.Confirm c = en.getValue().getFuture().get(remaining, TimeUnit.NANOSECONDS);
                if (c.isAck() && en.getValue().getReturned() == null) {
                    ok.add(en.getKey());
                } else {
                    log.warn("event {} not confirmed: ack={} returned={} reason={}", en.getKey(), c.isAck(),
                            en.getValue().getReturned() != null, c.getReason());
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                log.warn("event {} confirm failed: {}", en.getKey(), ex.toString());
            }
        }
        return ok;
    }

    @Override
    public void deadLetter(OutboxEvent event) {
        template.send(RabbitConfig.DLX, "dead." + event.eventType(), message(event));
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
