package io.pigeon.outbox;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class RabbitOutboxPublisherIntegrationTest {

    @Container
    static RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:3-management");

    static RabbitTemplate template;
    static RabbitOutboxPublisher publisher;

    @BeforeAll
    static void setup() {
        CachingConnectionFactory factory = new CachingConnectionFactory(
                rabbit.getHost(), rabbit.getAmqpPort());
        factory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        factory.setPublisherReturns(true);
        template = new RabbitTemplate(factory);
        template.setMandatory(true);

        RabbitAdmin admin = new RabbitAdmin(template);
        TopicExchange exchange = new TopicExchange("pigeon.events", true, false);
        Queue queue = new Queue("pigeon.test.orders", true);
        admin.declareExchange(exchange);
        admin.declareQueue(queue);
        admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("order.created"));

        publisher = new RabbitOutboxPublisher(template, OutboxProperties.defaults());
    }

    @Test
    void confirmedMessageIsDeliveredAndReportedConfirmed() {
        OutboxEvent event = event("order.created");

        PublishBatchResult result = publisher.publish(List.of(event));

        assertThat(result.get(event.id())).isEqualTo(PublishOutcome.CONFIRMED);
        assertThat(template.receive("pigeon.test.orders", 5_000)).isNotNull();
    }

    @Test
    void unroutableMessageIsReportedPermanent() {
        OutboxEvent event = event("order.unroutable");

        PublishBatchResult result = publisher.publish(List.of(event));

        assertThat(result.get(event.id())).isEqualTo(PublishOutcome.PERMANENT);
    }

    private static OutboxEvent event(String eventType) {
        return new OutboxEvent(UUID.randomUUID(), "order", "1", eventType,
                "{}", 0, OffsetDateTime.now());
    }
}
