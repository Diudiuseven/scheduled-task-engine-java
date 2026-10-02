package com.github.diudiuseven.scheduledtaskenginejava.engine.config;

import com.github.diudiuseven.scheduledtaskenginejava.engine.ScheduledTaskEngine;
import com.github.diudiuseven.scheduledtaskenginejava.engine.TaskDefinition;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.CatchUpPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RetryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.CronTrigger;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.IntervalTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

/**
 * 演示：启动引擎并注册两个示例任务。
 * engine.demo.enabled=false 可关闭。
 */
@Component
@ConditionalOnProperty(prefix = "engine.demo", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

    private final ScheduledTaskEngine engine;

    public DemoRunner(ScheduledTaskEngine engine) {
        this.engine = engine;
    }

    @Override
    public void run(ApplicationArguments args) {
        Instant start = engine.clock().instant();

        // 固定间隔任务：每 10 秒
        engine.register(TaskDefinition.builder("demo-interval",
                        IntervalTrigger.of(start, Duration.ofSeconds(10), ZoneId.of("UTC")),
                        ctx -> log.info("[demo-interval] fire={} attempt={}", ctx.getFireTime(), ctx.getAttempt()))
                .overlapPolicy(OverlapPolicy.SKIP)
                .catchUpPolicy(CatchUpPolicy.of(Duration.ofMinutes(5)))
                .retryPolicy(RetryPolicy.of(3, Duration.ofSeconds(1), Duration.ofSeconds(30), 2.0, 0.2))
                .startTime(start)
                .build());

        // cron 任务：每分钟第 0 秒，纽约时区
        engine.register(TaskDefinition.builder("demo-cron",
                        CronTrigger.of("0 * * * *", ZoneId.of("America/New_York")),
                        ctx -> log.info("[demo-cron] fire={} attempt={}", ctx.getFireTime(), ctx.getAttempt()))
                .overlapPolicy(OverlapPolicy.QUEUE)
                .catchUpPolicy(CatchUpPolicy.of(Duration.ofMinutes(10)))
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build());

        engine.start();
        log.info("Demo tasks registered and engine started");
    }
}
