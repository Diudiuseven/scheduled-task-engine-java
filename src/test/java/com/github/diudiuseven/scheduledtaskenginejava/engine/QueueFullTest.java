package com.github.diudiuseven.scheduledtaskenginejava.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.config.EngineProperties;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionRecord;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RetryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.SkipReason;
import com.github.diudiuseven.scheduledtaskenginejava.engine.support.Engines;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.IntervalTrigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 队列有界：超出队列容量的触发点记录为 QUEUE_FULL 跳过。
 */
class QueueFullTest {

    @TempDir
    Path dbDir;

    private ScheduledTaskEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Test
    void queueOverflow_skippedWithReason() throws Exception {
        EngineProperties props = Engines.fastProperties();
        props.setRateLimit(0);
        props.setQueueCapacity(2);
        props.setWorkerPoolSize(1); // 单 worker：第一个阻塞后，后续命令留在队列中
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), props);

        Instant start = Instant.now();
        CountDownLatch blocker = new CountDownLatch(1);
        TaskDefinition task = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> {
                            try {
                                blocker.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        })
                .overlapPolicy(OverlapPolicy.QUEUE)
                .maxConcurrency(1)
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
        engine.register(task);
        engine.start();

        // 先让第一个执行进入 RUNNING（阻塞），再入队其余
        engine.scheduleFirePoint("t1", start.plusSeconds(1));
        await().atMost(Duration.ofSeconds(5)).until(() ->
                engine.executionsOf("t1").stream()
                        .anyMatch(r -> r.getStatus() == ExecutionStatus.RUNNING));

        // 第一个占用工作线程，队列容量 2，后 4 个中 2 个入队、2 个被拒
        for (int i = 2; i <= 5; i++) {
            engine.scheduleFirePoint("t1", start.plusSeconds(i));
        }

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t1");
            long queued = records.stream()
                    .filter(r -> r.getStatus() == ExecutionStatus.QUEUED
                            || r.getStatus() == ExecutionStatus.PENDING
                            || r.getStatus() == ExecutionStatus.RUNNING)
                    .count();
            long rejected = records.stream()
                    .filter(r -> r.getStatus() == ExecutionStatus.SKIPPED)
                    .filter(r -> r.getSkipReason() == SkipReason.QUEUE_FULL)
                    .count();
            assertThat(queued + rejected).isEqualTo(5);
            assertThat(rejected).isEqualTo(2);
        });

        blocker.countDown();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t1");
            long terminal = records.stream().filter(r -> r.getStatus().isTerminal()).count();
            assertThat(terminal).isEqualTo(5);
        });
    }
}
