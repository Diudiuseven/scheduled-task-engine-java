package com.github.diudiuseven.scheduledtaskenginejava.tests.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.core.EngineConfig;
import com.github.diudiuseven.scheduledtaskenginejava.engine.core.SchedulerEngine;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskDefinition;
import com.github.diudiuseven.scheduledtaskenginejava.engine.policy.MisfirePolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.policy.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.InMemoryExecutionStore;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.FixedIntervalTrigger;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;
import static com.github.diudiuseven.scheduledtaskenginejava.tests.engine.TestSupport.*;

/**
 * 场景：停机或长时间停止后的补跑。
 */
class MisfireTest {

    private static final Instant T0 = Instant.parse("2024-06-01T00:00:00Z");

    private TaskDefinition task(String id, Duration catchUpWindow) {
        return TaskDefinition.builder(id,
                        new FixedIntervalTrigger(T0, Duration.ofMinutes(1), ZoneId.of("UTC")))
                .misfirePolicy(new MisfirePolicy(catchUpWindow))
                // 补跑的触发点会同时到期，QUEUE 保证它们依次串行执行而不是被重叠跳过
                .overlapPolicy(OverlapPolicy.QUEUE)
                .build();
    }

    private EngineConfig config() {
        return new EngineConfig(Duration.ofMillis(20), 1000.0, 16);
    }

    @Test
    void missedFireTimesWithinWindowAreCaughtUpAfterRestart() {
        MutableClock clock = new MutableClock(T0);
        InMemoryExecutionStore store = new InMemoryExecutionStore();
        ConcurrentLinkedQueue<Instant> executed = new ConcurrentLinkedQueue<>();

        // 第一阶段：引擎运行到 T0+90s，产生 T0+1min 一个触发点
        SchedulerEngine engine1 = new SchedulerEngine(config(), store, clock);
        engine1.register(task("t", Duration.ofMinutes(10)), e -> executed.add(e.getFireTime()));
        engine1.start();
        clock.advance(Duration.ofSeconds(90));
        await(() -> store.findByTask("t").size() == 1, "first fire recorded");
        engine1.close();

        // 模拟停机 5 分钟
        clock.advance(Duration.ofMinutes(5));

        // 第二阶段：重启后补算错过的触发点（T0+2min .. T0+6min 共 5 个）
        SchedulerEngine engine2 = new SchedulerEngine(config(), store, clock);
        engine2.register(task("t", Duration.ofMinutes(10)), e -> executed.add(e.getFireTime()));
        engine2.start();
        await(() -> store.findByTask("t").size() == 6, "missed fire times caught up");
        await(() -> executed.size() == 6, "all caught-up executions ran");
        engine2.close();

        List<Instant> fireTimes = store.findByTask("t").stream()
                .map(e -> e.getFireTime()).sorted().toList();
        for (int i = 0; i < 6; i++) {
            assertEquals(T0.plusSeconds(60L * (i + 1)), fireTimes.get(i));
        }
        assertTrue(store.findByTask("t").stream().allMatch(e -> e.getStatus() == ExecutionStatus.SUCCESS));
    }

    @Test
    void fireTimesBeyondWindowAreSkippedWithReason() {
        MutableClock clock = new MutableClock(T0);
        InMemoryExecutionStore store = new InMemoryExecutionStore();

        SchedulerEngine engine1 = new SchedulerEngine(config(), store, clock);
        engine1.register(task("t", Duration.ofMinutes(10)), e -> {
        });
        engine1.start();
        clock.advance(Duration.ofSeconds(90));
        await(() -> store.findByTask("t").size() == 1, "first fire recorded");
        engine1.close();

        // 停机 30 分钟，补跑窗口只有 10 分钟
        clock.advance(Duration.ofMinutes(30));

        SchedulerEngine engine2 = new SchedulerEngine(config(), store, clock);
        engine2.register(task("t", Duration.ofMinutes(10)), e -> {
        });
        engine2.start();
        // T0+1min .. T0+31min 共 31 个触发点
        await(() -> store.findByTask("t").size() == 31, "all missed fire times accounted");
        engine2.close();

        long skipped = store.findByTask("t").stream()
                .filter(e -> e.getStatus() == ExecutionStatus.SKIPPED)
                .filter(e -> "out-of-misfire-window".equals(e.getReason()))
                .count();
        long succeeded = store.findByTask("t").stream()
                .filter(e -> e.getStatus() == ExecutionStatus.SUCCESS)
                .count();
        // now = T0+31min，窗口 [T0+21min, T0+31min] 内 11 个执行成功，之前 20 个跳过
        assertEquals(20, skipped);
        assertEquals(11, succeeded);
    }
}
