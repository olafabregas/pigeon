package io.pigeon.core.order;

import io.pigeon.core.config.DemoRabbitConfig;
import io.pigeon.outbox.RabbitOutboxPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;

/** Sample idempotent consumer: dedupes on the messageId header. */
@Component
@ConditionalOnProperty(name = "pigeon.consumer.enabled", havingValue = "true", matchIfMissing = true)
public class OrderEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
    private final JdbcTemplate jdbc;

    public OrderEventConsumer(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @RabbitListener(queues = DemoRabbitConfig.ORDERS_QUEUE)
    @Transactional
    public void onMessage(Message message) {
        Object header = message.getMessageProperties().getHeader(RabbitOutboxPublisher.MESSAGE_ID_HEADER);
        if (header == null) {
            throw new IllegalArgumentException("missing messageId header");
        }
        String messageId = header.toString();
        int inserted = jdbc.update(
                "INSERT INTO processed_messages (message_id) VALUES (?) ON CONFLICT DO NOTHING", messageId);
        if (inserted == 0) {
            log.info("duplicate message {} ignored", messageId);
            return;
        }
        log.info("processed message {} body={}", messageId, new String(message.getBody(), StandardCharsets.UTF_8));
    }
}
