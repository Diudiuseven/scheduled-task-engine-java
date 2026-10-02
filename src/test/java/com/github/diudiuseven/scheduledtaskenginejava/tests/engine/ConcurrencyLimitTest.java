package com.github.diudiuseven.scheduledtaskenginejava.tests.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.core.EngineConfig;
import com.github.diudiuseven.scheduledtaskenginejava.engine.core.SchedulerEngine;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskDefinition;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskExecution;
import com.github.diudiuseven.scheduledtaskenginejava.engine.policy.RejectPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.InMemoryExecutionStore;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.FixedIntervalTrigger;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static com.github.diudiuseven.scheduledtaskenginejava.tests.engine.TestSupport.await;

/**
 * 场景：触发速率与并发度限制；超限按策略排队或拒绝并记录原因；
 * 取消进行中的执行不留半完成状态。
 */
class ConcurrencyLimitTest {

    private TaskDefinition task(String id, RejectPolicy policy) {
        return TaskDefinition.builder(id,
                        new FixedIntervalTrigger(Instant.now().plusSeconds(600), Duration.ofMinutes(10), ZoneId.of("UTC")))
                .rejectPolicy(policy)
                .build();
    }

    @Test
    void globalConcurrencyExcessIsRejectedWithReason() throws Exception {
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        CountDownLatch block = new CountDownLatch(1);
        CountDownLatch firstRunning = new CountDownLatch(1);

        // 全局并发只有 1：第二个任务的触发将被拒绝
        try (SchedulerEngine engine = new SchedulerEngine(
                new EngineConfig(Duration.ofMillis(20), 1000.0, 1), store, Clock.systemUTC())) {
            engine.register(task("a", RejectPolicy.REJECT), e -> {
                firstRunning.countDown();
                block.await(5, TimeUnit.SECONDS);
            });
            engine.register(task("b", RejectPolicy.REJECT), e -> {
            });
            engine.start();
            engine.triggerNow("a");
            assertTrue(firstRunning.await(5, TimeUnit.SECONDS));
            engine.triggerNow("b");

            await(() -> store.findByTask("b").stream()
                            .anyMatch(e -> e.getStatus() == ExecutionStatus.SKIPPED
                                    && "global-concurrency-limited".equals(e.getReason())),
                    "excess trigger rejected with reason recorded");
            block.countDown();
        }

        assertEquals(1, store.findByTask("b").size());
    }

    @Test
    void rateLimitExcessIsQueuedAndEventuallyRuns() throws Exception {
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        AtomicInteger runs = new AtomicInteger();

        // 全局速率 2 次/秒：连续 3 次触发，第 3 次进入队列等待
        try (SchedulerEngine engine = new SchedulerEngine(
                new EngineConfig(Duration.ofMillis(20), 2.0, 16), store, Clock.systemUTC())) {
            engine.register(task("t", RejectPolicy.QUEUE), e -> runs.incrementAndGet());
            engine.start();
            engine.triggerNow("t");
            Thread.sleep(5);
            engine.triggerNow("t");
            Thread.sleep(5);
            engine.triggerNow("t");

            await(() -> runs.get() == 3, "queued triggers eventually run after rate limit refill");
        }

        assertEquals(3, store.findByTask("t").size());
        assertTrue(store.findByTask("t").stream().allMatch(e -> e.getStatus() == ExecutionStatus.SUCCESS));
    }

    @Test
    void rateLimitExcessIsRejectedWhenPolicyIsReject() throws Exception {
        InMemoryExecutionStore store = new InMemoryExecutionStore();

        try (SchedulerEngine engine = new SchedulerEngine(
                new EngineConfig(Duration.ofMillis(20), 1.0, 16), store, Clock.systemUTC())) {
            engine.register(task("t", RejectPolicy.REJECT), e -> {
            });
            engine.start();
            // 连续触发打满令牌桶（容量 1），每次间隔 5ms 保证触发点互不相同
            for (int i = 0; i < 5; i++) {
                engine.triggerNow("t");
                Thread.sleep(5);
            }
            await(() -> store.findByTask("t").stream()
                            .anyMatch(e -> e.getStatus() == ExecutionStatus.SKIPPED
                                    && "rate-limited".equals(e.getReason())),
                    "excess triggers rejected with rate-limited reason");
        }
    }

    @Test
    void cancelRunningExecutionLeavesNoHalfDoneState() throws Exception {
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        try (SchedulerEngine engine = new SchedulerEngine(
                new EngineConfig(Duration.ofMillis(20), 1000.0, 16), store, Clock.systemUTC())) {
            engine.register(task("t", RejectPolicy.QUEUE), e -> {
                running.countDown();
                release.await(5, TimeUnit.SECONDS);
            });
            engine.start();
            engine.triggerNow("t");
            assertTrue(running.await(5, TimeUnit.SECONDS));

            TaskExecution exec = store.findByTask("t").get(0);
            assertTrue(engine.cancel("t", exec.getFireTime()), "cancel accepted for running execution");
            release.countDown();

            await(() -> store.findByTask("t").get(0).getStatus() == ExecutionStatus.CANCELLED,
                    "cancelled state is final");
            // 给 worker 完成回调留出时间，确认状态不会被改写为 SUCCESS
            Thread.sleep(200);
            assertEquals(ExecutionStatus.CANCELLED, store.findByTask("t").get(0).getStatus());
        }
    }

    @Test
    void cancelPendingExecutionPreventsExecution() throws Exception {
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        AtomicInteger runs = new AtomicInteger();

        // 速率 1 次/秒（令牌桶初始容量 1）：第一次触发立即执行，第二次进入队列
        try (SchedulerEngine engine = new SchedulerEngine(
                new EngineConfig(Duration.ofMillis(20), 1.0, 16), store, Clock.systemUTC())) {
            engine.register(task("t", RejectPolicy.QUEUE), e -> runs.incrementAndGet());
            engine.start();
            engine.triggerNow("t"); // 消耗令牌
            Thread.sleep(5);
            engine.triggerNow("t"); // 进入 pendingQueue

            TaskExecution queued = store.findByTask("t").stream()
                    .filter(e -> e.getStatus() == ExecutionStatus.PENDING)
                    .findFirst().orElseThrow();
            assertTrue(engine.cancel("t", queued.getFireTime()));
            assertEquals(ExecutionStatus.CANCELLED, queued.getStatus());

            // 等待足够长时间让令牌桶重新填满，确认被取消的执行不会运行
            Thread.sleep(1200);
            assertEquals(1, runs.get(), "cancelled queued execution never runs");
        }
    }
}
