package com.github.diudiuseven.scheduledtaskenginejava.tests.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.core.EngineConfig;
import com.github.diudiuseven.scheduledtaskenginejava.engine.core.SchedulerEngine;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskDefinition;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskExecution;
import com.github.diudiuseven.scheduledtaskenginejava.engine.policy.RetryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.InMemoryExecutionStore;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.FixedIntervalTrigger;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static com.github.diudiuseven.scheduledtaskenginejava.tests.engine.TestSupport.await;

/**
 * 场景：失败按退避策略重试；重试不阻塞其它任务。
 */
class RetryTest {

    private SchedulerEngine engine(InMemoryExecutionStore store) {
        return new SchedulerEngine(new EngineConfig(Duration.ofMillis(20), 1000.0, 16),
                store, Clock.systemUTC());
    }

    @Test
    void failedExecutionIsRetriedWithBackoffUntilSuccess() {
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        AtomicInteger calls = new AtomicInteger();

        try (SchedulerEngine engine = engine(store)) {
            engine.register(TaskDefinition.builder("t",
                            new FixedIntervalTrigger(Instant.now().plusSeconds(60), Duration.ofMinutes(1), ZoneId.of("UTC")))
                            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, Duration.ofMillis(500)))
                            .build(),
                    e -> {
                        if (calls.incrementAndGet() < 3) {
                            throw new IllegalStateException("boom " + calls.get());
                        }
                    });
            engine.start();
            assertTrue(engine.triggerNow("t"));
            await(() -> store.findByTask("t").stream()
                            .anyMatch(e -> e.getStatus() == ExecutionStatus.SUCCESS),
                    "execution eventually succeeds after retries");
        }

        TaskExecution exec = store.findByTask("t").get(0);
        assertEquals(3, calls.get(), "handler called 3 times (2 failures + 1 success)");
        assertEquals(3, exec.getAttempt());
        assertEquals(ExecutionStatus.SUCCESS, exec.getStatus());
    }

    @Test
    void executionFailsPermanentlyAfterRetriesExhausted() {
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        AtomicInteger calls = new AtomicInteger();

        try (SchedulerEngine engine = engine(store)) {
            engine.register(TaskDefinition.builder("t",
                            new FixedIntervalTrigger(Instant.now().plusSeconds(60), Duration.ofMinutes(1), ZoneId.of("UTC")))
                            .retryPolicy(new RetryPolicy(2, Duration.ofMillis(30), 1.0, Duration.ofMillis(30)))
                            .build(),
                    e -> {
                        calls.incrementAndGet();
                        throw new IllegalStateException("always fails");
                    });
            engine.start();
            assertTrue(engine.triggerNow("t"));
            await(() -> store.findByTask("t").stream()
                            .anyMatch(e -> e.getStatus() == ExecutionStatus.FAILED),
                    "execution marked FAILED after retries exhausted");
        }

        assertEquals(2, calls.get(), "no more attempts after maxAttempts reached");
        TaskExecution exec = store.findByTask("t").get(0);
        assertTrue(exec.getReason().contains("always fails"));
    }

    @Test
    void retryBackoffDoesNotBlockOtherTasks() {
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        AtomicInteger otherTaskRuns = new AtomicInteger();

        try (SchedulerEngine engine = engine(store)) {
            // 任务 flaky：失败后等待 500ms 再重试
            engine.register(TaskDefinition.builder("flaky",
                            new FixedIntervalTrigger(Instant.now().plusSeconds(60), Duration.ofMinutes(1), ZoneId.of("UTC")))
                            .retryPolicy(new RetryPolicy(5, Duration.ofMillis(500), 1.0, Duration.ofMillis(500)))
                            .build(),
                    e -> {
                        throw new IllegalStateException("boom");
                    });
            // 任务 healthy：每 50ms 正常触发
            engine.register(TaskDefinition.builder("healthy",
                            new FixedIntervalTrigger(Instant.now().plusMillis(50), Duration.ofMillis(50), ZoneId.of("UTC")))
                            .build(),
                    e -> otherTaskRuns.incrementAndGet());
            engine.start();
            assertTrue(engine.triggerNow("flaky"));
            // 在 flaky 处于退避等待期间，healthy 应持续触发
            await(() -> otherTaskRuns.get() >= 3, "healthy task keeps firing during other's backoff");
            List<TaskExecution> healthyExecs = store.findByTask("healthy");
            assertTrue(healthyExecs.stream().allMatch(e -> e.getStatus() == ExecutionStatus.SUCCESS));
        }
    }
}
