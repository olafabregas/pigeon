package io.pigeon.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.OffsetDateTime;
import java.net.ConnectException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RabbitOutboxPublisherTest {

    @Test
    void sendFailureOnlyAffectsTheEventThatFailed() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        when(template.getConnectionFactory()).thenReturn(connectionFactory);
        when(connectionFactory.createConnection()).thenReturn(connection);
        AtomicInteger sends = new AtomicInteger();
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(3);
            if (sends.incrementAndGet() == 2) {
                throw new AmqpException("permanent send failure");
            }
            correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(template).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));

        List<OutboxEvent> events = List.of(event(), event(), event());
        PublishBatchResult result = new RabbitOutboxPublisher(template, OutboxProperties.defaults())
                .publish(events);

        assertThat(result.get(events.get(0).id())).isEqualTo(PublishOutcome.CONFIRMED);
        assertThat(result.get(events.get(1).id())).isEqualTo(PublishOutcome.PERMANENT);
        assertThat(result.get(events.get(2).id())).isEqualTo(PublishOutcome.CONFIRMED);
        assertThat(sends).hasValue(3);
    }

    @Test
    void transientSendFailureOnlyDefersTheEventThatFailed() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        when(template.getConnectionFactory()).thenReturn(connectionFactory);
        when(connectionFactory.createConnection()).thenReturn(connection);
        AtomicInteger sends = new AtomicInteger();
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(3);
            if (sends.incrementAndGet() == 2) {
                throw new AmqpIOException("connection reset", new ConnectException("connection reset"));
            }
            correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(template).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));

        List<OutboxEvent> events = List.of(event(), event(), event());
        PublishBatchResult result = new RabbitOutboxPublisher(template, OutboxProperties.defaults())
                .publish(events);

        assertThat(result.get(events.get(0).id())).isEqualTo(PublishOutcome.CONFIRMED);
        assertThat(result.get(events.get(1).id())).isEqualTo(PublishOutcome.TRANSIENT);
        assertThat(result.get(events.get(2).id())).isEqualTo(PublishOutcome.CONFIRMED);
        assertThat(sends).hasValue(3);
    }

    private static OutboxEvent event() {
        return new OutboxEvent(UUID.randomUUID(), "order", "1", "order.created",
                "{}", 0, OffsetDateTime.now());
    }
}
