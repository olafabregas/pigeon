package io.pigeon.core.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {
    public static final String EXCHANGE = "pigeon.events";
    public static final String QUEUE = "pigeon.orders";
    public static final String DLX = "pigeon.dlx";
    public static final String DLQ = "pigeon.dlq";

    @Bean
    TopicExchange eventsExchange() {
        return ExchangeBuilder.topicExchange(EXCHANGE).durable(true).build();
    }

    @Bean
    TopicExchange deadLetterExchange() {
        return ExchangeBuilder.topicExchange(DLX).durable(true).build();
    }

    @Bean
    Queue ordersQueue() {
        return QueueBuilder.durable(QUEUE).deadLetterExchange(DLX).deadLetterRoutingKey("dead.order").build();
    }

    @Bean
    Queue deadLetterQueue() {
        return QueueBuilder.durable(DLQ).build();
    }

    @Bean
    Binding ordersBinding() {
        return BindingBuilder.bind(ordersQueue()).to(eventsExchange()).with("order.*");
    }

    @Bean
    Binding dlqBinding() {
        return BindingBuilder.bind(deadLetterQueue()).to(deadLetterExchange()).with("dead.#");
    }
}
