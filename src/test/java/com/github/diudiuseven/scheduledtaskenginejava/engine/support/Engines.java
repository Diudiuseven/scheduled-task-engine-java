package com.github.diudiuseven.scheduledtaskenginejava.engine.support;

import com.github.diudiuseven.scheduledtaskenginejava.engine.ScheduledTaskEngine;
import com.github.diudiuseven.scheduledtaskenginejava.engine.config.EngineProperties;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.ExecutionStore;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.JdbcExecutionStore;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.Clock;

/**
 * 测试用引擎工厂：基于临时目录的 H2 文件库，可跨“重启”共享数据。
 */
public final class Engines {

    private Engines() {
    }

    public static ScheduledTaskEngine fileBacked(Path dbDir, Clock clock, EngineProperties props) {
        DataSource ds = DataSourceBuilder.create()
                .url("jdbc:h2:file:" + dbDir.resolve("engine").toAbsolutePath()
                        + ";DB_CLOSE_DELAY=-1;AUTO_SERVER=TRUE")
                .driverClassName("org.h2.Driver")
                .username("sa")
                .password("")
                .build();
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        ExecutionStore store = new JdbcExecutionStore(jdbc);
        return new ScheduledTaskEngine(clock, store, props);
    }

    public static EngineProperties fastProperties() {
        EngineProperties p = new EngineProperties();
        p.setWorkerPoolSize(4);
        p.setRateLimit(0); // 默认不限速，限速场景单独设置
        p.setQueueCapacity(100);
        p.setTickInterval(java.time.Duration.ofMillis(50));
        p.setAcquireTimeout(java.time.Duration.ofMillis(500));
        return p;
    }
}
