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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 触发速率与单任务并发度限制：超出排队或拒绝并记录原因；取消不留下半完成状态。
 */
class ConcurrencyTest {

    @TempDir
    Path dbDir;

    private ScheduledTaskEngine engine;

    private ScheduledTaskEngine engineWith(EngineProperties props) {
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), props);
        return engine;
    }

    private EngineProperties props(double rateLimit, Duration acquireTimeout) {
        EngineProperties p = Engines.fastProperties();
        p.setRateLimit(rateLimit);
        p.setAcquireTimeout(acquireTimeout);
        return p;
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Test
    void rateLimit_smoothsExecutionStarts() {
        engineWith(props(2.0, Duration.ofSeconds(10)));
        Instant start = Instant.now();
        CopyOnWriteArrayList<Long> startTimes = new CopyOnWriteArrayList<>();
        TaskDefinition task = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> startTimes.add(System.currentTimeMillis()))
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
        engine.register(task);
        engine.start();
        for (int i = 1; i <= 5; i++) {
            engine.scheduleFirePoint("t1", start.plusSeconds(i));
        }

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(startTimes).hasSize(5));

        for (int i = 1; i < startTimes.size(); i++) {
            assertThat(startTimes.get(i) - startTimes.get(i - 1)).isGreaterThanOrEqualTo(400);
        }
    }

    @Test
    void rateLimit_whenWaitExceedsTimeout_rejectedWithReason() {
        engineWith(props(1.0, Duration.ofMillis(100)));
        Instant start = Instant.now();
        TaskDefinition task = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> { })
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
        engine.register(task);
        engine.start();
        for (int i = 1; i <= 10; i++) {
            engine.scheduleFirePoint("t1", start.plusSeconds(i));
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t1");
            long succeeded = records.stream().filter(r -> r.getStatus() == ExecutionStatus.SUCCEEDED).count();
            long rejected = records.stream()
                    .filter(r -> r.getStatus() == ExecutionStatus.SKIPPED)
                    .filter(r -> r.getSkipReason() == SkipReason.RATE_LIMITED)
                    .count();
            assertThat(succeeded).isGreaterThanOrEqualTo(1);
            assertThat(rejected).isGreaterThanOrEqualTo(5);
        });
    }

    @Test
    void perTaskConcurrency_serializesWithQueue() throws Exception {
        engineWith(props(0, Duration.ofSeconds(5)));
        Instant start = Instant.now();
        CopyOnWriteArrayList<long[]> intervals = new CopyOnWriteArrayList<>();
        CountDownLatch firstStarted = new CountDownLatch(1);
        TaskDefinition task = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> {
                            long s = System.nanoTime();
                            firstStarted.countDown();
                            try {
                                Thread.sleep(200);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            intervals.add(new long[]{s, System.nanoTime()});
                        })
                .overlapPolicy(OverlapPolicy.QUEUE)
                .maxConcurrency(1)
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
        engine.register(task);
        engine.start();
        for (int i = 1; i <= 3; i++) {
            engine.scheduleFirePoint("t1", start.plusSeconds(i));
        }

        assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
        // 第一个执行期间，最多一个 RUNNING（轮询等待，避免固定 sleep 的时序竞态）
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            long running = engine.executionsOf("t1").stream()
                    .filter(r -> r.getStatus() == ExecutionStatus.RUNNING).count();
            assertThat(running).isEqualTo(1);
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t1");
            assertThat(records).hasSize(3);
            assertThat(records).allSatisfy(r ->
                    assertThat(r.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED));
        });
        // 区间互不重叠
        for (int i = 1; i < intervals.size(); i++) {
            assertThat(intervals.get(i)[0]).isGreaterThanOrEqualTo(intervals.get(i - 1)[1]);
        }
    }

    @Test
    void cancelRunning_marksCancelled_noHalfState() throws Exception {
        engineWith(props(0, Duration.ofSeconds(5)));
        Instant start = Instant.now();
        CountDownLatch blocker = new CountDownLatch(1);
        TaskDefinition task = TaskDefinition.builder("t1",
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
        engine.register(task);
        engine.start();
        engine.scheduleFirePoint("t1", start.plusSeconds(1));

        await().atMost(Duration.ofSeconds(5)).until(() ->
                engine.executionsOf("t1").stream()
                        .anyMatch(r -> r.getStatus() == ExecutionStatus.RUNNING));

        ExecutionRecord running = engine.executionsOf("t1").stream()
                .filter(r -> r.getStatus() == ExecutionStatus.RUNNING).findFirst().orElseThrow();
        boolean cancelled = engine.cancel(running.getId());
        assertThat(cancelled).isTrue();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            ExecutionRecord rec = engine.store().findById(running.getId()).orElseThrow();
            assertThat(rec.getStatus()).isEqualTo(ExecutionStatus.CANCELLED);
        });
        // 不存在 RUNNING 残留
        assertThat(engine.executionsOf("t1")).noneMatch(r -> r.getStatus() == ExecutionStatus.RUNNING);
    }

    @Test
    void cancelQueued_removesFromQueue() throws Exception {
        engineWith(props(0, Duration.ofSeconds(5)));
        Instant start = Instant.now();
        CountDownLatch blocker = new CountDownLatch(1);
        TaskDefinition task = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> {
                            try {
                                blocker.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        })
                .overlapPolicy(OverlapPolicy.QUEUE)
                .maxConcurrency(1)
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
        engine.register(task);
        engine.start();
        engine.scheduleFirePoint("t1", start.plusSeconds(1));
        engine.scheduleFirePoint("t1", start.plusSeconds(2));

        await().atMost(Duration.ofSeconds(5)).until(() ->
                engine.executionsOf("t1").stream()
                        .anyMatch(r -> r.getStatus() == ExecutionStatus.RUNNING));

        // 第二个在队列中（信号量被第一个占用）
        ExecutionRecord queued = engine.executionsOf("t1").stream()
                .filter(r -> r.getStatus() == ExecutionStatus.QUEUED
                        || r.getStatus() == ExecutionStatus.PENDING)
                .findFirst().orElseThrow();
        boolean cancelled = engine.cancel(queued.getId());
        assertThat(cancelled).isTrue();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(engine.store().findById(queued.getId()).orElseThrow().getStatus())
                        .isEqualTo(ExecutionStatus.CANCELLED));

        blocker.countDown();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t1");
            assertThat(records).hasSize(2);
            assertThat(records).anySatisfy(r -> assertThat(r.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED));
            assertThat(records).anySatisfy(r -> assertThat(r.getStatus()).isEqualTo(ExecutionStatus.CANCELLED));
        });
    }
}
