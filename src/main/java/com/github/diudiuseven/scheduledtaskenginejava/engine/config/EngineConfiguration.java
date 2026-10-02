package com.github.diudiuseven.scheduledtaskenginejava.engine.config;

import com.github.diudiuseven.scheduledtaskenginejava.engine.ScheduledTaskEngine;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.ExecutionStore;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.JdbcExecutionStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.time.Clock;

/**
 * 引擎自动配置：H2 文件库持久化 + 引擎 Bean。
 */
@Configuration
@EnableConfigurationProperties(EngineProperties.class)
public class EngineConfiguration {

    @Bean
    public Clock engineClock() {
        return Clock.systemUTC();
    }

    @Bean
    public DataSource engineDataSource(EngineProperties properties) {
        return DataSourceBuilder.create()
                .url("jdbc:h2:file:" + properties.getDbPath()
                        + ";MODE=Regular;DB_CLOSE_DELAY=-1;AUTO_SERVER=TRUE")
                .driverClassName("org.h2.Driver")
                .username("sa")
                .password("")
                .build();
    }

    @Bean
    public JdbcTemplate engineJdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    public ExecutionStore executionStore(JdbcTemplate engineJdbcTemplate) {
        return new JdbcExecutionStore(engineJdbcTemplate);
    }

    @Bean
    public ScheduledTaskEngine scheduledTaskEngine(Clock engineClock,
                                                   ExecutionStore executionStore,
                                                   EngineProperties properties) {
        return new ScheduledTaskEngine(engineClock, executionStore, properties);
    }
}
