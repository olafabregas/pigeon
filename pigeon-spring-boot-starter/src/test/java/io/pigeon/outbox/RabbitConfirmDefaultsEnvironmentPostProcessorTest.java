package io.pigeon.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitConfirmDefaultsEnvironmentPostProcessorTest {

    private final RabbitConfirmDefaultsEnvironmentPostProcessor processor =
            new RabbitConfirmDefaultsEnvironmentPostProcessor();

    @Test
    void setsDefaultsWhenUnset() {
        StandardEnvironment env = new StandardEnvironment();
        processor.postProcessEnvironment(env, new SpringApplication());

        assertThat(env.getProperty("spring.rabbitmq.publisher-confirm-type")).isEqualTo("correlated");
        assertThat(env.getProperty("spring.rabbitmq.publisher-returns")).isEqualTo("true");
        assertThat(env.getProperty("spring.rabbitmq.template.mandatory")).isEqualTo("true");
        assertThat(env.getPropertySources().contains(
                RabbitConfirmDefaultsEnvironmentPostProcessor.PROPERTY_SOURCE_NAME)).isTrue();
    }

    @Test
    void doesNotOverrideExplicitUserConfig() {
        StandardEnvironment env = new StandardEnvironment();
        env.getPropertySources().addFirst(new MapPropertySource("user", Map.of(
                "spring.rabbitmq.publisher-confirm-type", "none",
                "spring.rabbitmq.publisher-returns", "false",
                "spring.rabbitmq.template.mandatory", "false")));

        processor.postProcessEnvironment(env, new SpringApplication());

        assertThat(env.getProperty("spring.rabbitmq.publisher-confirm-type")).isEqualTo("none");
        assertThat(env.getProperty("spring.rabbitmq.publisher-returns")).isEqualTo("false");
        assertThat(env.getProperty("spring.rabbitmq.template.mandatory")).isEqualTo("false");
        // no pigeon defaults source when everything was already set
        assertThat(env.getPropertySources().contains(
                RabbitConfirmDefaultsEnvironmentPostProcessor.PROPERTY_SOURCE_NAME)).isFalse();
    }
}
