package io.pigeon.core;

import io.pigeon.core.outbox.OutboxProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(OutboxProperties.class)
public class PigeonCoreApplication {
    public static void main(String[] args) {
        SpringApplication.run(PigeonCoreApplication.class, args);
    }
}
