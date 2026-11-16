package io.pigeon.core.config;

import io.pigeon.outbox.OutboxProperties;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Demo Rabbit topology: exchange/DLQ plus orders queue (starter auto-config is excluded here). */
@Configuration
public class DemoRabbitConfig {
    public static final String ORDERS_QUEUE = "pigeon.orders";

    @Bean
    TopicExchange pigeonEventsExchange(OutboxProperties props) {
        return ExchangeBuilder.topicExchange(props.exchange()).durable(true).build();
    }

    @Bean
    TopicExchange pigeonDeadLetterExchange(OutboxProperties props) {
        return ExchangeBuilder.topicExchange(props.deadLetterExchange()).durable(true).build();
    }

    @Bean
    Queue pigeonDeadLetterQueue(OutboxProperties props) {
        return QueueBuilder.durable(props.deadLetterQueue()).build();
    }

    @Bean
    Binding pigeonDlqBinding(Queue pigeonDeadLetterQueue, TopicExchange pigeonDeadLetterExchange) {
        return BindingBuilder.bind(pigeonDeadLetterQueue).to(pigeonDeadLetterExchange).with("dead.#");
    }

    @Bean
    Queue ordersQueue(OutboxProperties props) {
        return QueueBuilder.durable(ORDERS_QUEUE)
                .deadLetterExchange(props.deadLetterExchange())
                .deadLetterRoutingKey("dead.order")
                .build();
    }

    @Bean
    Binding ordersBinding(Queue ordersQueue, TopicExchange pigeonEventsExchange) {
        return BindingBuilder.bind(ordersQueue).to(pigeonEventsExchange).with("order.*");
    }
}
