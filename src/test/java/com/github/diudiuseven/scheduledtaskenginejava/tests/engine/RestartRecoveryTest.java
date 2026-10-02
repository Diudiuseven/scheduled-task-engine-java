package com.github.diudiuseven.scheduledtaskenginejava.tests.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.core.EngineConfig;
import com.github.diudiuseven.scheduledtaskenginejava.engine.core.SchedulerEngine;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskDefinition;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskExecution;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.FileExecutionStore;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.FixedIntervalTrigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static com.github.diudiuseven.scheduledtaskenginejava.tests.engine.TestSupport.await;

/**
 * 场景：触发与执行状态持久化；重启后区分已完成 / 在途 / 失败，
 * 未完成的执行被恢复且不重复、不遗漏。
 */
class RestartRecoveryTest {

    @TempDir
    Path dir;

    private TaskDefinition task(String id) {
        return TaskDefinition.builder(id,
                        new FixedIntervalTrigger(Instant.now().plusSeconds(600), Duration.ofMinutes(10), ZoneId.of("UTC")))
                .build();
    }

    @Test
    void stateIsPersistedAndUnfinishedExecutionsAreRecovered() throws Exception {
        Path storeFile = dir.resolve("executions.json");
        CountDownLatch runningLatch = new CountDownLatch(1);
        CountDownLatch blockLatch = new CountDownLatch(1);

        // 第一阶段：执行进入 RUNNING 后引擎被关闭（模拟宕机）
        SchedulerEngine engine1 = new SchedulerEngine(
                new EngineConfig(Duration.ofMillis(20), 1000.0, 16),
                new FileExecutionStore(storeFile), Clock.systemUTC());
        engine1.register(task("t"), e -> {
            runningLatch.countDown();
            blockLatch.await(5, TimeUnit.SECONDS);
        });
        engine1.start();
        assertTrue(engine1.triggerNow("t"));
        assertTrue(runningLatch.await(5, TimeUnit.SECONDS), "execution entered RUNNING");
        await(() -> new FileExecutionStore(storeFile).findByTask("t").stream()
                        .anyMatch(e -> e.getStatus() == ExecutionStatus.RUNNING),
                "RUNNING state persisted to disk");
        engine1.close(); // 关闭时在途执行落库为 PENDING(shutdown-interrupted)，不留半完成状态
        blockLatch.countDown();

        // 验证持久化文件中可区分状态
        FileExecutionStore reloaded = new FileExecutionStore(storeFile);
        List<TaskExecution> unfinished = reloaded.findUnfinished();
        assertEquals(1, unfinished.size());
        assertEquals(ExecutionStatus.PENDING, unfinished.get(0).getStatus());
        assertEquals("shutdown-interrupted", unfinished.get(0).getReason());

        // 第二阶段：重启后恢复未完成的执行，且只执行一次
        AtomicInteger recoveredRuns = new AtomicInteger();
        SchedulerEngine engine2 = new SchedulerEngine(
                new EngineConfig(Duration.ofMillis(20), 1000.0, 16),
                new FileExecutionStore(storeFile), Clock.systemUTC());
        engine2.register(task("t"), e -> recoveredRuns.incrementAndGet());
        engine2.start();
        await(() -> new FileExecutionStore(storeFile).findByTask("t").stream()
                        .anyMatch(e -> e.getStatus() == ExecutionStatus.SUCCESS),
                "recovered execution completes after restart");
        engine2.close();

        assertEquals(1, recoveredRuns.get(), "recovered exactly once, no duplicate execution");
        List<TaskExecution> all = new FileExecutionStore(storeFile).findByTask("t");
        assertEquals(1, all.size(), "still a single execution record for the fire point");
        assertEquals(ExecutionStatus.SUCCESS, all.get(0).getStatus());
        assertEquals(2, all.get(0).getAttempt(), "attempt count survived the restart");
    }

    @Test
    void finishedAndFailedExecutionsAreNotRecovered() {
        Path storeFile = dir.resolve("executions.json");

        SchedulerEngine engine1 = new SchedulerEngine(
                new EngineConfig(Duration.ofMillis(20), 1000.0, 16),
                new FileExecutionStore(storeFile), Clock.systemUTC());
        AtomicInteger runs = new AtomicInteger();
        engine1.register(task("ok"), e -> runs.incrementAndGet());
        engine1.register(TaskDefinition.builder("bad",
                        new FixedIntervalTrigger(Instant.now().plusSeconds(600), Duration.ofMinutes(10), ZoneId.of("UTC")))
                        .retryPolicy(com.github.diudiuseven.scheduledtaskenginejava.engine.policy.RetryPolicy.NO_RETRY)
                        .build(),
                e -> {
                    throw new IllegalStateException("permanent failure");
                });
        engine1.start();
        engine1.triggerNow("ok");
        engine1.triggerNow("bad");
        await(() -> new FileExecutionStore(storeFile).findByTask("ok").stream()
                        .anyMatch(e -> e.getStatus() == ExecutionStatus.SUCCESS)
                        && new FileExecutionStore(storeFile).findByTask("bad").stream()
                        .anyMatch(e -> e.getStatus() == ExecutionStatus.FAILED),
                "terminal states persisted");
        engine1.close();

        // 重启：已完成与已失败的执行不得被恢复重跑
        SchedulerEngine engine2 = new SchedulerEngine(
                new EngineConfig(Duration.ofMillis(20), 1000.0, 16),
                new FileExecutionStore(storeFile), Clock.systemUTC());
        engine2.register(task("ok"), e -> runs.incrementAndGet());
        engine2.register(task("bad"), e -> runs.incrementAndGet());
        engine2.start();
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        engine2.close();

        assertEquals(1, runs.get(), "terminal executions must not be re-run after restart");
    }
}
