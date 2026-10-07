package com.github.diudiuseven.scheduledtaskenginejava.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.config.EngineProperties;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionRecord;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RetryPolicy;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 时序回归测试：并发登记与执行同时进行、完成后不重跑、重启恢复不重复、
 * 大量任务同时到点时不丢触发不空等。
 */
class RaceRegressionTest {

    @TempDir
    Path dbDir;

    private ScheduledTaskEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    private static TaskDefinition countingTask(String id, Instant start, AtomicInteger counter) {
        return TaskDefinition.builder(id,
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> counter.incrementAndGet())
                .overlapPolicy(OverlapPolicy.QUEUE)
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
    }

    /** 多线程同时登记同一触发点且引擎正在执行：恰好一次有效执行，其余记为重复。 */
    @Test
    void concurrentRegistrationAndExecution_exactlyOneExecution() {
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), Engines.fastProperties());
        Instant start = Instant.now();
        AtomicInteger executions = new AtomicInteger();
        engine.register(countingTask("t1", start, executions));
        engine.start();

        Instant fire = start.plusSeconds(5);
        int threads = 16;
        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                try {
                    gate.await();
                    if (engine.scheduleFirePoint("t1", fire)) {
                        accepted.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }
        gate.countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> done.getCount() == 0);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t1");
            assertThat(records).hasSize(1);
            assertThat(records.get(0).getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        });
        assertThat(accepted.get()).isEqualTo(1);
        assertThat(executions.get()).isEqualTo(1);
        // 其余 15 次全部被识别为重复触发（而非其它原因拒绝）
        assertThat(engine.store().countDuplicateEvents("t1")).isEqualTo(threads - 1);
    }

    /** 触发点执行完成后：重启不重跑、副作用不重复，再次登记记为重复。 */
    @Test
    void succeededFirePoint_notRerunAfterRestart() {
        Instant start = Instant.now();
        AtomicInteger executions = new AtomicInteger(); // 跨“重启”共享，统计真实副作用次数

        ScheduledTaskEngine engine1 = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), Engines.fastProperties());
        engine1.register(countingTask("t1", start, executions));
        engine1.start();
        Instant fire = start.plusSeconds(5);
        engine1.scheduleFirePoint("t1", fire);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(engine1.executionsOf("t1").get(0).getStatus())
                        .isEqualTo(ExecutionStatus.SUCCEEDED));
        engine1.stop();
        assertThat(executions.get()).isEqualTo(1);

        // 重启：同一 DB。已完成触发点不得被恢复重跑
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), Engines.fastProperties());
        engine.register(countingTask("t1", start, executions));
        engine.start();

        // 恢复流程结束后再次确认：没有重跑，状态仍是 SUCCEEDED
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(engine.store().findRecoverable()).isEmpty());
        assertThat(executions.get()).isEqualTo(1);
        assertThat(engine.executionsOf("t1")).allSatisfy(r ->
                assertThat(r.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED));

        // 已完成的触发点再次登记：拒绝并记为重复触发
        assertThat(engine.scheduleFirePoint("t1", fire)).isFalse();
        assertThat(engine.store().countDuplicateEvents("t1")).isEqualTo(1);
        assertThat(executions.get()).isEqualTo(1);
    }

    /** 终态（成功/失败/取消）记录重启后不得被重新捡起执行。 */
    @Test
    void terminalRecords_neverRecoveredAfterRestart() throws Exception {
        EngineProperties props = Engines.fastProperties();
        props.setWorkerPoolSize(1); // 单 worker：制造确定的“排队中”窗口
        Instant start = Instant.now();
        CountDownLatch blocker = new CountDownLatch(1);

        TaskDefinition blocking = TaskDefinition.builder("t-block",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> {
                            try {
                                blocker.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        })
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
        TaskDefinition failing = TaskDefinition.builder("t-fail",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> { throw new RuntimeException("always fails"); })
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
        AtomicInteger okRuns = new AtomicInteger();
        TaskDefinition ok = countingTask("t-ok", start, okRuns);
        AtomicInteger cancelRuns = new AtomicInteger();
        TaskDefinition toCancel = countingTask("t-cancel", start, cancelRuns);

        ScheduledTaskEngine engine1 = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), props);
        engine1.register(blocking);
        engine1.register(failing);
        engine1.register(ok);
        engine1.register(toCancel);
        engine1.start();

        engine1.scheduleFirePoint("t-block", start.plusSeconds(1));
        await().atMost(Duration.ofSeconds(5)).until(() ->
                engine1.executionsOf("t-block").stream()
                        .anyMatch(r -> r.getStatus() == ExecutionStatus.RUNNING));

        // worker 被占用，以下触发点确定地排队
        engine1.scheduleFirePoint("t-cancel", start.plusSeconds(1));
        engine1.scheduleFirePoint("t-fail", start.plusSeconds(1));
        engine1.scheduleFirePoint("t-ok", start.plusSeconds(1));

        ExecutionRecord queued = engine1.executionsOf("t-cancel").get(0);
        assertThat(engine1.cancel(queued.getId())).isTrue();
        blocker.countDown();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(engine1.executionsOf("t-block").get(0).getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
            assertThat(engine1.executionsOf("t-fail").get(0).getStatus()).isEqualTo(ExecutionStatus.FAILED);
            assertThat(engine1.executionsOf("t-ok").get(0).getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
            assertThat(engine1.executionsOf("t-cancel").get(0).getStatus()).isEqualTo(ExecutionStatus.CANCELLED);
        });
        engine1.stop();

        // 重启：所有记录均为终态，恢复集合必须为空，任何处理器都不得再跑
        AtomicInteger rerun = new AtomicInteger();
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), props);
        engine.register(countingTask("t-block", start, rerun));
        engine.register(countingTask("t-fail", start, rerun));
        engine.register(countingTask("t-ok", start, rerun));
        engine.register(countingTask("t-cancel", start, rerun));
        engine.start();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(engine.store().findRecoverable()).isEmpty());
        assertThat(rerun.get()).isZero();
        assertThat(engine.executionsOf("t-fail").get(0).getStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(engine.executionsOf("t-cancel").get(0).getStatus()).isEqualTo(ExecutionStatus.CANCELLED);
    }

    /** 大量任务同时到点、数量接近队列容量：不丢触发、不空等，全部到达终态且恰好执行一次。 */
    @Test
    void massTriggerNearQueueCapacity_noLossNoStall() {
        EngineProperties props = Engines.fastProperties();
        props.setWorkerPoolSize(4);
        props.setQueueCapacity(100);
        props.setRateLimit(0);
        // 等待预算充足：容量内、不限速时任何触发点都不得被拒绝或丢失
        props.setAcquireTimeout(Duration.ofSeconds(5));
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), props);

        Instant start = Instant.now();
        int taskCount = 10;
        int pointsPerTask = 9; // 共 90 条，接近容量 100
        AtomicInteger executions = new AtomicInteger();
        for (int t = 0; t < taskCount; t++) {
            engine.register(countingTask("task-" + t, start, executions));
        }
        engine.start();

        for (int t = 0; t < taskCount; t++) {
            for (int p = 1; p <= pointsPerTask; p++) {
                assertThat(engine.scheduleFirePoint("task-" + t, start.plusSeconds(p))).isTrue();
            }
        }

        int total = taskCount * pointsPerTask;
        // 所有触发点必须在有限时间内到达终态（不允许卡在等待上）
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            long terminal = 0;
            for (int t = 0; t < taskCount; t++) {
                terminal += engine.executionsOf("task-" + t).stream()
                        .filter(r -> r.getStatus().isTerminal()).count();
            }
            assertThat(terminal).isEqualTo(total);
        });
        // 容量足够且不限速：全部成功，且每个触发点恰好执行一次
        assertThat(executions.get()).isEqualTo(total);
        for (int t = 0; t < taskCount; t++) {
            assertThat(engine.executionsOf("task-" + t)).allSatisfy(r ->
                    assertThat(r.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED));
        }
    }
}
