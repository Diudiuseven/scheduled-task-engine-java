package com.github.diudiuseven.scheduledtaskenginejava.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionRecord;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RetryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.SkipReason;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.ExecutionStore;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 状态单调性回归：执行进入运行/终态后，任何并发记账动作都不得把状态改回等待；
 * 同一触发点无论并发登记、重试还是重启恢复，最多一次有效执行。
 */
class StateMonotonicityTest {

    @TempDir
    Path dbDir;

    private ScheduledTaskEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    private TaskDefinition countingTask(String id, Instant start, AtomicInteger counter) {
        return TaskDefinition.builder(id,
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> counter.incrementAndGet())
                .overlapPolicy(OverlapPolicy.QUEUE)
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
    }

    @Test
    void terminalStateCannotBeOverwrittenByBookkeeping() {
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), Engines.fastProperties());
        ExecutionStore store = engine.store();
        Instant now = Instant.now();

        // SUCCEEDED 终态：后续排队/运行/重试/取消记账全部不生效
        ExecutionRecord rec = store.insertPending("t1", now, now, 1).orElseThrow();
        assertThat(store.markQueued(rec.getId(), now)).isTrue();
        assertThat(store.markRunning(rec.getId(), now)).isTrue();
        assertThat(store.markSucceeded(rec.getId(), now)).isTrue();
        assertThat(store.markQueued(rec.getId(), now)).isFalse();
        assertThat(store.markRunning(rec.getId(), now)).isFalse();
        assertThat(store.markRetryWait(rec.getId(), now, "x")).isFalse();
        assertThat(store.markCancelled(rec.getId(), now)).isFalse();
        assertThat(store.markSkipped(rec.getId(), now, SkipReason.OVERLAP)).isFalse();
        assertThat(store.findById(rec.getId()).orElseThrow().getStatus())
                .isEqualTo(ExecutionStatus.SUCCEEDED);

        // CANCELLED 终态：不被成功/失败/重新排队覆盖
        ExecutionRecord rec2 = store.insertPending("t1", now.plusSeconds(1), now, 1).orElseThrow();
        assertThat(store.markCancelled(rec2.getId(), now)).isTrue();
        assertThat(store.markQueued(rec2.getId(), now)).isFalse();
        assertThat(store.markRunning(rec2.getId(), now)).isFalse();
        assertThat(store.markSucceeded(rec2.getId(), now)).isFalse();
        assertThat(store.markFailed(rec2.getId(), now, "x")).isFalse();
        assertThat(store.findById(rec2.getId()).orElseThrow().getStatus())
                .isEqualTo(ExecutionStatus.CANCELLED);

        // FAILED 终态：不被任何后续动作覆盖
        ExecutionRecord rec3 = store.insertPending("t1", now.plusSeconds(2), now, 1).orElseThrow();
        assertThat(store.markRunning(rec3.getId(), now)).isTrue();
        assertThat(store.markFailed(rec3.getId(), now, "boom")).isTrue();
        assertThat(store.markQueued(rec3.getId(), now)).isFalse();
        assertThat(store.markRunning(rec3.getId(), now)).isFalse();
        assertThat(store.markSucceeded(rec3.getId(), now)).isFalse();
        assertThat(store.findById(rec3.getId()).orElseThrow().getStatus())
                .isEqualTo(ExecutionStatus.FAILED);
    }

    @Test
    void concurrentRegisterAndExecute_completedRecordsStaySucceeded() throws Exception {
        com.github.diudiuseven.scheduledtaskenginejava.engine.config.EngineProperties props =
                Engines.fastProperties();
        // 充分的并发与等待预算：本测试只关注状态单调性，不触发限流/并发拒绝
        props.setAcquireTimeout(Duration.ofSeconds(30));
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), props);
        Instant start = Instant.now();
        AtomicInteger handlerRuns = new AtomicInteger();
        engine.register(TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> handlerRuns.incrementAndGet())
                .overlapPolicy(OverlapPolicy.QUEUE)
                .maxConcurrency(8)
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build());
        engine.start();

        // 多线程并发登记触发点，worker 几乎同时开始执行：
        // 复现“先 offer 后 markQueued”与执行完成之间的竞态窗口
        int points = 200;
        ExecutorService producers = Executors.newFixedThreadPool(4);
        CountDownLatch gate = new CountDownLatch(1);
        for (int p = 0; p < 4; p++) {
            int offset = p * (points / 4);
            producers.submit(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < points / 4; i++) {
                    engine.scheduleFirePoint("t1", start.plusSeconds(offset + i + 1));
                }
            });
        }
        gate.countDown();
        producers.shutdown();
        assertThat(producers.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        // 所有记录都必须到达终态；若完成态被改回 QUEUED/PENDING，此处会超时失败
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t1");
            assertThat(records).hasSize(points);
            assertThat(records).allMatch(r -> r.getStatus().isTerminal());
        });

        List<ExecutionRecord> records = engine.executionsOf("t1");
        long succeeded = records.stream()
                .filter(r -> r.getStatus() == ExecutionStatus.SUCCEEDED).count();
        // 持久化结果与真实执行一致：每次 handler 运行都对应一条 SUCCEEDED
        assertThat(succeeded).isEqualTo(points);
        assertThat(handlerRuns.get()).isEqualTo(points);
    }

    @Test
    void sameFirePointConcurrentScheduling_exactlyOneExecution() throws Exception {
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), Engines.fastProperties());
        Instant start = Instant.now();
        AtomicInteger handlerRuns = new AtomicInteger();
        engine.register(countingTask("t1", start, handlerRuns));
        engine.start();

        Instant fire = start.plusSeconds(5);
        int threads = 16;
        AtomicInteger accepted = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
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
            });
        }
        gate.countDown();
        assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        // 恰好一个调用被接受，其余全部识别为重复并落库
        assertThat(accepted.get()).isEqualTo(1);
        assertThat(engine.store().countDuplicateEvents("t1")).isEqualTo(threads - 1);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(engine.executionsOf("t1").get(0).getStatus())
                        .isEqualTo(ExecutionStatus.SUCCEEDED));
        assertThat(handlerRuns.get()).isEqualTo(1);
        assertThat(engine.executionsOf("t1")).hasSize(1);
    }

    @Test
    void completedFirePoint_notReexecutedAfterRestart() {
        Instant start = Instant.now();
        AtomicInteger handlerRuns = new AtomicInteger();

        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), Engines.fastProperties());
        engine.register(countingTask("t1", start, handlerRuns));
        engine.start();
        Instant fire = start.plusSeconds(5);
        engine.scheduleFirePoint("t1", fire);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(engine.executionsOf("t1").get(0).getStatus())
                        .isEqualTo(ExecutionStatus.SUCCEEDED));
        engine.stop();

        // 重启：已完成的触发点不得被重新捡起
        ScheduledTaskEngine engine2 = Engines.fileBacked(
                dbDir, java.time.Clock.systemUTC(), Engines.fastProperties());
        try {
            engine2.register(countingTask("t1", start, handlerRuns));
            engine2.start();
            await().during(Duration.ofMillis(800)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(handlerRuns.get()).isEqualTo(1));
            List<ExecutionRecord> records = engine2.executionsOf("t1");
            assertThat(records).hasSize(1);
            assertThat(records.get(0).getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        } finally {
            engine2.stop();
        }
    }

    @Test
    void terminalRecords_notPickedUpByRecovery() {
        Instant start = Instant.now();
        // 预置四种终态记录（引擎未启动，直接写库）
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), Engines.fastProperties());
        ExecutionStore store = engine.store();
        ExecutionRecord succeeded = store.insertPending("t1", start.plusSeconds(1), start, 1).orElseThrow();
        store.markRunning(succeeded.getId(), start);
        store.markSucceeded(succeeded.getId(), start);
        ExecutionRecord failed = store.insertPending("t1", start.plusSeconds(2), start, 1).orElseThrow();
        store.markRunning(failed.getId(), start);
        store.markFailed(failed.getId(), start, "boom");
        ExecutionRecord cancelled = store.insertPending("t1", start.plusSeconds(3), start, 1).orElseThrow();
        store.markCancelled(cancelled.getId(), start);
        ExecutionRecord skipped = store.insertPending("t1", start.plusSeconds(4), start, 1).orElseThrow();
        store.markSkipped(skipped.getId(), start, SkipReason.OVERLAP);
        engine.stop();

        assertThat(store.findRecoverable()).isEmpty();

        AtomicInteger handlerRuns = new AtomicInteger();
        ScheduledTaskEngine engine2 = Engines.fileBacked(
                dbDir, java.time.Clock.systemUTC(), Engines.fastProperties());
        try {
            engine2.register(countingTask("t1", start, handlerRuns));
            engine2.start();
            await().during(Duration.ofMillis(800)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(handlerRuns.get()).isZero());
            List<ExecutionRecord> records = engine2.executionsOf("t1");
            assertThat(records).hasSize(4);
            assertThat(records).allMatch(r -> r.getStatus().isTerminal());
        } finally {
            engine2.stop();
        }
    }
}
