package com.github.diudiuseven.scheduledtaskenginejava.tests.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.core.EngineConfig;
import com.github.diudiuseven.scheduledtaskenginejava.engine.core.EngineListener;
import com.github.diudiuseven.scheduledtaskenginejava.engine.core.SchedulerEngine;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskDefinition;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.InMemoryExecutionStore;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.FixedIntervalTrigger;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static com.github.diudiuseven.scheduledtaskenginejava.tests.engine.TestSupport.await;

/**
 * 场景：同一触发点被重复调度（手动触发与周期触发撞点、补跑重放）时，
 * 只产生一次有效执行，重复触发被识别并记录。
 */
class DuplicateTriggerTest {

    @Test
    void duplicateFirePointProducesSingleExecution() {
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        Instant fixed = Instant.parse("2024-06-01T12:00:00Z");
        AtomicInteger handlerCalls = new AtomicInteger();
        List<String> duplicates = new CopyOnWriteArrayList<>();

        try (SchedulerEngine engine = new SchedulerEngine(
                new EngineConfig(Duration.ofMillis(20), 1000.0, 16),
                store, Clock.fixed(fixed, ZoneId.of("UTC")))) {
            engine.setListener(new EngineListener() {
                @Override
                public void onDuplicate(String taskId, Instant fireTime) {
                    duplicates.add(taskId + "@" + fireTime);
                }
            });
            engine.register(TaskDefinition.builder("t",
                            new FixedIntervalTrigger(fixed.plusSeconds(3600), Duration.ofHours(1), ZoneId.of("UTC")))
                            .build(),
                    e -> handlerCalls.incrementAndGet());
            engine.start();

            assertTrue(engine.triggerNow("t"), "first trigger creates execution");
            assertFalse(engine.triggerNow("t"), "second trigger of same fire point is duplicate");
            assertFalse(engine.triggerNow("t"), "third trigger of same fire point is duplicate");

            await(() -> handlerCalls.get() == 1, "handler executed exactly once");
        }

        assertEquals(1, store.findByTask("t").size(), "only one execution record exists");
        assertEquals(ExecutionStatus.SUCCESS, store.findByTask("t").get(0).getStatus());
        assertEquals(2, duplicates.size(), "both duplicate triggers are detected and recorded");
        assertEquals(1, handlerCalls.get());
    }

    @Test
    void catchUpDoesNotReexecuteAlreadyRecordedFirePoints() {
        // 模拟“补跑重放”：同一触发点已存在记录时，重启补算不会产生第二次执行
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        Instant t0 = Instant.parse("2024-06-01T00:00:00Z");
        TestSupport.MutableClock clock = new TestSupport.MutableClock(t0);
        AtomicInteger handlerCalls = new AtomicInteger();

        SchedulerEngine engine1 = new SchedulerEngine(
                new EngineConfig(Duration.ofMillis(20), 1000.0, 16), store, clock);
        TaskDefinition def = TaskDefinition.builder("t",
                        new FixedIntervalTrigger(t0, Duration.ofMinutes(1), ZoneId.of("UTC")))
                .build();
        engine1.register(def, e -> handlerCalls.incrementAndGet());
        engine1.start();
        clock.advance(Duration.ofSeconds(90));
        await(() -> store.findByTask("t").size() == 1, "first fire recorded");
        engine1.close();

        // 时钟只前进了 20 秒（now = T0+110s < 下一触发点 T0+120s）：
        // 没有新的触发点，重启补算不应重复执行已记录的触发点
        clock.advance(Duration.ofSeconds(20));
        SchedulerEngine engine2 = new SchedulerEngine(
                new EngineConfig(Duration.ofMillis(20), 1000.0, 16), store, clock);
        engine2.register(def, e -> handlerCalls.incrementAndGet());
        engine2.start();
        // 给补算逻辑足够的时间运行
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        engine2.close();

        assertEquals(1, store.findByTask("t").size(), "no duplicate record after restart");
        assertEquals(1, handlerCalls.get(), "no duplicate execution after restart");
    }
}
