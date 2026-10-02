package com.github.diudiuseven.scheduledtaskenginejava.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionRecord;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RetryPolicy;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 失败按退避策略重试；重试在延迟队列中进行，不阻塞其它任务。
 */
class RetryTest {

    @TempDir
    Path dbDir;

    private ScheduledTaskEngine engine;

    @BeforeEach
    void setUp() {
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), Engines.fastProperties());
    }

    @AfterEach
    void tearDown() {
        engine.stop();
    }

    @Test
    void failsTwiceThenSucceeds_backoffObserved() {
        Instant start = Instant.now();
        AtomicInteger attempts = new AtomicInteger();
        List<Long> attemptTimes = new ArrayList<>();
        TaskDefinition task = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> {
                            attemptTimes.add(System.currentTimeMillis());
                            if (attempts.incrementAndGet() < 3) {
                                throw new RuntimeException("boom " + attempts.get());
                            }
                        })
                .overlapPolicy(OverlapPolicy.QUEUE)
                .retryPolicy(RetryPolicy.of(3, Duration.ofMillis(30), Duration.ofMillis(200), 2.0, 0.0))
                .startTime(start)
                .build();
        engine.register(task);
        engine.start();
        engine.scheduleFirePoint("t1", start.plusSeconds(1));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            ExecutionRecord rec = engine.executionsOf("t1").get(0);
            assertThat(rec.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        });

        ExecutionRecord rec = engine.executionsOf("t1").get(0);
        assertThat(rec.getAttempt()).isEqualTo(2); // 失败 2 次后成功
        assertThat(attemptTimes).hasSize(3);
        // 退避间隔：>=30ms, >=60ms
        assertThat(attemptTimes.get(1) - attemptTimes.get(0)).isGreaterThanOrEqualTo(25);
        assertThat(attemptTimes.get(2) - attemptTimes.get(1)).isGreaterThanOrEqualTo(55);
    }

    @Test
    void retriesDoNotBlockOtherTasks() {
        Instant start = Instant.now();
        AtomicInteger aAttempts = new AtomicInteger();
        TaskDefinition taskA = TaskDefinition.builder("A",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> {
                            aAttempts.incrementAndGet();
                            throw new RuntimeException("A always fails");
                        })
                .overlapPolicy(OverlapPolicy.QUEUE)
                .retryPolicy(RetryPolicy.of(5, Duration.ofMillis(100), Duration.ofMillis(500), 2.0, 0.0))
                .startTime(start)
                .build();
        AtomicInteger bCount = new AtomicInteger();
        TaskDefinition taskB = TaskDefinition.builder("B",
                        IntervalTrigger.of(start, Duration.ofMillis(50), ZoneOffset.UTC),
                        ctx -> bCount.incrementAndGet())
                .overlapPolicy(OverlapPolicy.QUEUE)
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
        engine.register(taskA);
        engine.register(taskB);
        engine.start();
        engine.scheduleFirePoint("A", start.plusSeconds(1));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(bCount.get()).isGreaterThanOrEqualTo(5));
        // A 仍在重试（未阻塞 B）
        assertThat(aAttempts.get()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void exhaustedRetries_markedFailed() {
        Instant start = Instant.now();
        TaskDefinition task = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> {
                            throw new RuntimeException("permanent failure");
                        })
                .overlapPolicy(OverlapPolicy.QUEUE)
                .retryPolicy(RetryPolicy.of(2, Duration.ofMillis(10), Duration.ofMillis(100), 2.0, 0.0))
                .startTime(start)
                .build();
        engine.register(task);
        engine.start();
        engine.scheduleFirePoint("t1", start.plusSeconds(1));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            ExecutionRecord rec = engine.executionsOf("t1").get(0);
            assertThat(rec.getStatus()).isEqualTo(ExecutionStatus.FAILED);
        });
        ExecutionRecord rec = engine.executionsOf("t1").get(0);
        assertThat(rec.getAttempt()).isEqualTo(2);
        assertThat(rec.getLastError()).contains("permanent failure");
    }
}
