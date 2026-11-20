package io.pigeon.outbox;

import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.InitializingBean;

import javax.sql.DataSource;

final class PigeonFlywayMigration implements InitializingBean {
    private final DataSource dataSource;
    private final OutboxProperties properties;

    PigeonFlywayMigration(DataSource dataSource, OutboxProperties properties) {
        this.dataSource = dataSource;
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() {
        Flyway.configure()
                .dataSource(dataSource)
                .locations(properties.flywayLocations().split(","))
                .table(properties.flywayTable())
                .baselineOnMigrate(true)
                .load()
                .migrate();
    }
}
