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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 边界行为回归：队列容量上限、单任务非重叠约束、限流突发。
 * 触发与执行几乎同时发生时，不丢触发、不重叠、不空等，超限按策略拒绝并记录原因。
 */
class EngineBoundaryTest {

    @TempDir
    Path dbDir;

    private ScheduledTaskEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    private TaskDefinition.Builder taskBuilder(String id, Instant start, TaskHandler handler) {
        return TaskDefinition.builder(id,
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC), handler)
                .overlapPolicy(OverlapPolicy.QUEUE)
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start);
    }

    @Test
    void queueBoundary_concurrentProducers_noTriggerLost_overflowRejectedWithReason() throws Exception {
        EngineProperties props = Engines.fastProperties();
        props.setWorkerPoolSize(1);
        props.setQueueCapacity(5);
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), props);

        Instant start = Instant.now();
        CountDownLatch blocker = new CountDownLatch(1);
        AtomicInteger handlerRuns = new AtomicInteger();
        engine.register(taskBuilder("t1", start, ctx -> {
                    handlerRuns.incrementAndGet();
                    try {
                        blocker.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).maxConcurrency(1).build());
        engine.start();

        // 多线程同时登记 12 个触发点，队列容量只有 5
        int total = 12;
        ExecutorService producers = Executors.newFixedThreadPool(4);
        CountDownLatch gate = new CountDownLatch(1);
        for (int p = 0; p < 4; p++) {
            int offset = p * 3;
            producers.submit(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < 3; i++) {
                    engine.scheduleFirePoint("t1", start.plusSeconds(offset + i + 1));
                }
            });
        }
        gate.countDown();
        producers.shutdown();
        assertThat(producers.awaitTermination(15, TimeUnit.SECONDS)).isTrue();

        // 触发不丢失：每个触发点都有且仅有一条记录
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(engine.executionsOf("t1")).hasSize(total));

        List<ExecutionRecord> records = engine.executionsOf("t1");
        long skipped = records.stream().filter(r -> r.getStatus() == ExecutionStatus.SKIPPED).count();
        long accepted = total - skipped;
        // 超容量部分全部被拒绝且原因明确；接受数不超过 容量 + 在途(1 worker)
        assertThat(records.stream()
                .filter(r -> r.getStatus() == ExecutionStatus.SKIPPED))
                .allMatch(r -> r.getSkipReason() == SkipReason.QUEUE_FULL);
        assertThat(accepted).isLessThanOrEqualTo(5 + 1);
        assertThat(skipped).isEqualTo(total - accepted);

        // 放行后被接受的执行全部成功完成，状态不到达终态的记录不存在
        blocker.countDown();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(engine.executionsOf("t1")).allMatch(r -> r.getStatus().isTerminal()));
        assertThat(handlerRuns.get()).isEqualTo((int) accepted);
        assertThat(engine.executionsOf("t1").stream()
                .filter(r -> r.getStatus() == ExecutionStatus.SUCCEEDED).count())
                .isEqualTo(accepted);
    }

    @Test
    void overlapConstraint_triggerAndExecuteSimultaneously_neverOverlaps() throws Exception {
        EngineProperties props = Engines.fastProperties();
        props.setAcquireTimeout(Duration.ofMillis(250));
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), props);

        Instant start = Instant.now();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        engine.register(taskBuilder("t1", start, ctx -> {
                    int now = inFlight.incrementAndGet();
                    maxInFlight.accumulateAndGet(now, Math::max);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        inFlight.decrementAndGet();
                    }
                }).maxConcurrency(1).build());
        engine.start();

        // 触发与执行几乎同时发生：先制造一个长执行，执行期间并发登记多个触发点
        engine.scheduleFirePoint("t1", start.plusSeconds(1));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(engine.executionsOf("t1").stream()
                        .anyMatch(r -> r.getStatus() == ExecutionStatus.RUNNING)).isTrue());

        engine.scheduleFirePoint("t1", start.plusSeconds(2));
        engine.scheduleFirePoint("t1", start.plusSeconds(3));
        Thread.sleep(300);
        // 长执行仍在进行时再登记两个：此时若并发许可泄漏，它们会重叠执行
        engine.scheduleFirePoint("t1", start.plusSeconds(4));
        engine.scheduleFirePoint("t1", start.plusSeconds(5));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(engine.executionsOf("t1")).hasSize(5)
                        .allMatch(r -> r.getStatus().isTerminal()));

        // 单任务任何时刻最多一个在执行
        assertThat(maxInFlight.get()).isEqualTo(1);

        List<ExecutionRecord> records = engine.executionsOf("t1");
        // 第一个触发点成功；其余等待超时，各自留下可追溯的跳过记录与原因
        assertThat(records.stream()
                .filter(r -> r.getStatus() == ExecutionStatus.SUCCEEDED).count()).isEqualTo(1);
        assertThat(records.stream()
                .filter(r -> r.getStatus() == ExecutionStatus.SKIPPED).count()).isEqualTo(4);
        assertThat(records.stream()
                .filter(r -> r.getStatus() == ExecutionStatus.SKIPPED))
                .allMatch(r -> r.getSkipReason() == SkipReason.CONCURRENCY_LIMITED);
    }

    @Test
    void rateLimitBurst_allTriggersSettle_noStarvation() {
        EngineProperties props = Engines.fastProperties();
        props.setRateLimit(20.0); // 20/s：10 个突发触发点最长等待约 450ms
        props.setAcquireTimeout(Duration.ofSeconds(2));
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), props);

        Instant start = Instant.now();
        AtomicInteger handlerRuns = new AtomicInteger();
        engine.register(taskBuilder("t1", start, ctx -> handlerRuns.incrementAndGet())
                .maxConcurrency(4).build());
        engine.start();

        int total = 10;
        for (int i = 1; i <= total; i++) {
            engine.scheduleFirePoint("t1", start.plusSeconds(i));
        }

        // 限流下排队等待，但不得空等超时：全部在预算内成功完成
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(engine.executionsOf("t1")).hasSize(total);
            assertThat(engine.executionsOf("t1"))
                    .allMatch(r -> r.getStatus() == ExecutionStatus.SUCCEEDED);
        });
        assertThat(handlerRuns.get()).isEqualTo(total);
    }
}
