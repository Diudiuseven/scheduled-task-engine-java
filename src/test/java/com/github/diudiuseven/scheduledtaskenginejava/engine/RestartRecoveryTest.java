package com.github.diudiuseven.scheduledtaskenginejava.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionRecord;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RecoveryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RetryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.support.Engines;
import com.github.diudiuseven.scheduledtaskenginejava.engine.support.MutableClock;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.IntervalTrigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 重启恢复：在途执行按策略恢复，不重复执行、不漏触发点。
 */
class RestartRecoveryTest {

    @TempDir
    Path dbDir;

    private ScheduledTaskEngine engineAt(Path dir, MutableClock clock, TaskDefinition task) {
        ScheduledTaskEngine engine = Engines.fileBacked(dir, clock, Engines.fastProperties());
        engine.register(task);
        return engine;
    }

    @Test
    void runningExecution_recoveredAndRetried() {
        MutableClock clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        Instant start = clock.instant();
        CountDownLatch blocker = new CountDownLatch(1);

        TaskDefinition blocking = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> {
                            try {
                                blocker.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        })
                .overlapPolicy(OverlapPolicy.QUEUE)
                .retryPolicy(RetryPolicy.of(3, Duration.ofMillis(20), Duration.ofMillis(100), 2.0, 0.0))
                .recoveryPolicy(RecoveryPolicy.RETRY)
                .startTime(start)
                .build();

        ScheduledTaskEngine engine1 = engineAt(dbDir, clock, blocking);
        engine1.start();
        engine1.scheduleFirePoint("t1", start.plusSeconds(5));

        await().atMost(Duration.ofSeconds(5)).until(() ->
                engine1.executionsOf("t1").stream()
                        .anyMatch(r -> r.getStatus() == ExecutionStatus.RUNNING));

        // 停止引擎（中断在途执行），记录应保持 RUNNING
        engine1.stop();
        await().atMost(Duration.ofSeconds(2)).until(() ->
                engine1.executionsOf("t1").stream()
                        .anyMatch(r -> r.getStatus() == ExecutionStatus.RUNNING));

        // 重启：同一 DB，任务改为快速成功处理器
        TaskDefinition succeeding = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> { })
                .overlapPolicy(OverlapPolicy.QUEUE)
                .retryPolicy(RetryPolicy.of(3, Duration.ofMillis(20), Duration.ofMillis(100), 2.0, 0.0))
                .recoveryPolicy(RecoveryPolicy.RETRY)
                .startTime(start)
                .build();
        ScheduledTaskEngine engine2 = engineAt(dbDir, clock, succeeding);
        engine2.start();

        try {
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                List<ExecutionRecord> records = engine2.executionsOf("t1");
                assertThat(records).hasSize(1);
                assertThat(records.get(0).getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
            });
            ExecutionRecord rec = engine2.executionsOf("t1").get(0);
            assertThat(rec.getAttempt()).isEqualTo(1); // 中断的运行计为一次失败尝试
            // 不产生重复记录
            assertThat(engine2.store().countDuplicateEvents("t1")).isZero();
        } finally {
            engine2.stop();
        }
    }

    @Test
    void runningExecution_failedWhenRecoveryPolicyFail() {
        MutableClock clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        Instant start = clock.instant();
        CountDownLatch blocker = new CountDownLatch(1);

        TaskDefinition blocking = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> {
                            try {
                                blocker.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        })
                .retryPolicy(RetryPolicy.of(3, Duration.ofMillis(20), Duration.ofMillis(100), 2.0, 0.0))
                .recoveryPolicy(RecoveryPolicy.FAIL)
                .startTime(start)
                .build();

        ScheduledTaskEngine engine1 = engineAt(dbDir, clock, blocking);
        engine1.start();
        engine1.scheduleFirePoint("t1", start.plusSeconds(5));
        await().atMost(Duration.ofSeconds(5)).until(() ->
                engine1.executionsOf("t1").stream()
                        .anyMatch(r -> r.getStatus() == ExecutionStatus.RUNNING));
        engine1.stop();

        TaskDefinition succeeding = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> { })
                .retryPolicy(RetryPolicy.of(3, Duration.ofMillis(20), Duration.ofMillis(100), 2.0, 0.0))
                .recoveryPolicy(RecoveryPolicy.FAIL)
                .startTime(start)
                .build();
        ScheduledTaskEngine engine2 = engineAt(dbDir, clock, succeeding);
        engine2.start();

        try {
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                List<ExecutionRecord> records = engine2.executionsOf("t1");
                assertThat(records).hasSize(1);
                assertThat(records.get(0).getStatus()).isEqualTo(ExecutionStatus.FAILED);
            });
        } finally {
            engine2.stop();
        }
    }

    @Test
    void restart_noDuplicateNoMissedFirePoints() {
        MutableClock clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        Instant start = clock.instant();
        AtomicInteger count = new AtomicInteger();

        TaskDefinition task = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(10), ZoneOffset.UTC),
                        ctx -> count.incrementAndGet())
                .overlapPolicy(OverlapPolicy.QUEUE)
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();

        ScheduledTaskEngine engine1 = engineAt(dbDir, clock, task);
        engine1.start();
        clock.advance(Duration.ofSeconds(25)); // 触发 10s、20s 两个点
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(engine1.executionsOf("t1")).hasSize(2));
        engine1.stop();

        // 重启：同一 DB，时钟接续
        MutableClock clock2 = new MutableClock(clock.instant());
        ScheduledTaskEngine engine2 = engineAt(dbDir, clock2, task);
        engine2.start();
        clock2.advance(Duration.ofSeconds(10)); // 触发 30s 点
        try {
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                List<ExecutionRecord> records = engine2.executionsOf("t1");
                assertThat(records).hasSize(3);
                assertThat(records).extracting(ExecutionRecord::getFireTime)
                        .containsExactly(start.plusSeconds(10), start.plusSeconds(20), start.plusSeconds(30));
                assertThat(records).allSatisfy(r ->
                        assertThat(r.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED));
            });
        } finally {
            engine2.stop();
        }
    }
}
