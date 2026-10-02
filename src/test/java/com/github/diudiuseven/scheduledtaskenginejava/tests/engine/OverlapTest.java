package com.github.diudiuseven.scheduledtaskenginejava.tests.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.core.EngineConfig;
import com.github.diudiuseven.scheduledtaskenginejava.engine.core.SchedulerEngine;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskDefinition;
import com.github.diudiuseven.scheduledtaskenginejava.engine.policy.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.InMemoryExecutionStore;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.FixedIntervalTrigger;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static com.github.diudiuseven.scheduledtaskenginejava.tests.engine.TestSupport.await;

/**
 * 场景：执行耗时超过周期时的重叠处理；任一时刻同一任务最多一个有效执行。
 */
class OverlapTest {

    private SchedulerEngine engine(InMemoryExecutionStore store) {
        return new SchedulerEngine(new EngineConfig(Duration.ofMillis(20), 1000.0, 16),
                store, Clock.systemUTC());
    }

    private TaskDefinition task(String id, OverlapPolicy policy, AtomicInteger inFlight, AtomicInteger maxSeen) {
        return TaskDefinition.builder(id, new FixedIntervalTrigger(
                        Instant.now().plusMillis(50), Duration.ofMillis(100), ZoneId.of("UTC")))
                .overlapPolicy(policy)
                .build();
    }

    @Test
    void skipPolicySkipsOverlappingFiresAndNeverRunsConcurrently() {
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxSeen = new AtomicInteger();

        try (SchedulerEngine engine = engine(store)) {
            engine.register(task("t", OverlapPolicy.SKIP, inFlight, maxSeen), e -> {
                int now = inFlight.incrementAndGet();
                maxSeen.accumulateAndGet(now, Math::max);
                Thread.sleep(250); // 执行耗时 250ms > 周期 100ms
                inFlight.decrementAndGet();
            });
            engine.start();
            await(() -> store.findByTask("t").size() >= 8, "enough fire points generated");
        }

        long skippedOverlap = store.findByTask("t").stream()
                .filter(e -> e.getStatus() == ExecutionStatus.SKIPPED)
                .filter(e -> "overlap".equals(e.getReason()))
                .count();
        long succeeded = store.findByTask("t").stream()
                .filter(e -> e.getStatus() == ExecutionStatus.SUCCESS)
                .count();
        assertTrue(skippedOverlap > 0, "expected some fires skipped due to overlap");
        assertTrue(succeeded > 0, "expected some executions to succeed");
        assertEquals(1, maxSeen.get(), "at most one active execution at any time");
    }

    @Test
    void queuePolicySerializesOverlappingFires() {
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxSeen = new AtomicInteger();

        try (SchedulerEngine engine = engine(store)) {
            engine.register(task("t", OverlapPolicy.QUEUE, inFlight, maxSeen), e -> {
                int now = inFlight.incrementAndGet();
                maxSeen.accumulateAndGet(now, Math::max);
                Thread.sleep(150);
                inFlight.decrementAndGet();
            });
            engine.start();
            await(() -> store.findByTask("t").stream()
                            .filter(e -> e.getStatus() == ExecutionStatus.SUCCESS).count() >= 3,
                    "queued executions eventually run");
        }

        assertEquals(1, maxSeen.get(), "queue policy still guarantees one active execution");
        long skippedOverlap = store.findByTask("t").stream()
                .filter(e -> e.getStatus() == ExecutionStatus.SKIPPED)
                .filter(e -> "overlap".equals(e.getReason()))
                .count();
        assertEquals(0, skippedOverlap, "queue policy must not skip overlapping fires");
    }
}
