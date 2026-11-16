package io.pigeon.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Soft-defaults Rabbit publisher confirms/returns/mandatory when the user has not set them.
 * Explicit application.yml / env values always win.
 */
@Order(Ordered.LOWEST_PRECEDENCE)
public class RabbitConfirmDefaultsEnvironmentPostProcessor implements EnvironmentPostProcessor {

    static final String PROPERTY_SOURCE_NAME = "pigeonRabbitConfirmDefaults";

    private static final String CONFIRM_TYPE = "spring.rabbitmq.publisher-confirm-type";
    private static final String PUBLISHER_RETURNS = "spring.rabbitmq.publisher-returns";
    private static final String MANDATORY = "spring.rabbitmq.template.mandatory";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        if (environment.getPropertySources().contains(PROPERTY_SOURCE_NAME)) {
            return;
        }
        Map<String, Object> defaults = new LinkedHashMap<>();
        if (!environment.containsProperty(CONFIRM_TYPE)) {
            defaults.put(CONFIRM_TYPE, "correlated");
        }
        if (!environment.containsProperty(PUBLISHER_RETURNS)) {
            defaults.put(PUBLISHER_RETURNS, "true");
        }
        if (!environment.containsProperty(MANDATORY)) {
            defaults.put(MANDATORY, "true");
        }
        if (!defaults.isEmpty()) {
            // addLast = lowest precedence so user config overrides
            environment.getPropertySources().addLast(new MapPropertySource(PROPERTY_SOURCE_NAME, defaults));
        }
    }
}
