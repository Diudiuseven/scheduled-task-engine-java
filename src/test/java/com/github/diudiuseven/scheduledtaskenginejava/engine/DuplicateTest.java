package com.github.diudiuseven.scheduledtaskenginejava.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionRecord;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 同一触发点被重复调度（重试/补跑/手动）时只产生一次有效执行，重复事件落库。
 */
class DuplicateTest {

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
    void sameFirePointScheduledTwice_onlyOneExecution() {
        Instant start = Instant.now();
        AtomicInteger executions = new AtomicInteger();
        TaskDefinition task = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> executions.incrementAndGet())
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
        engine.register(task);
        engine.start();

        Instant fire = start.plusSeconds(5);
        boolean first = engine.scheduleFirePoint("t1", fire);
        boolean second = engine.scheduleFirePoint("t1", fire);

        assertThat(first).isTrue();
        assertThat(second).isFalse();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t1");
            assertThat(records).hasSize(1);
            assertThat(records.get(0).getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        });

        assertThat(executions.get()).isEqualTo(1);
        assertThat(engine.store().countDuplicateEvents("t1")).isEqualTo(1);
    }

    @Test
    void retryDoesNotCreateNewExecution() {
        Instant start = Instant.now();
        AtomicInteger attempts = new AtomicInteger();
        TaskDefinition task = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(60), ZoneOffset.UTC),
                        ctx -> {
                            if (attempts.incrementAndGet() < 3) {
                                throw new RuntimeException("boom");
                            }
                        })
                .retryPolicy(RetryPolicy.of(3, Duration.ofMillis(20), Duration.ofMillis(100), 2.0, 0.0))
                .startTime(start)
                .build();
        engine.register(task);
        engine.start();
        engine.scheduleFirePoint("t1", start.plusSeconds(1));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t1");
            assertThat(records).hasSize(1);
            assertThat(records.get(0).getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED);
        });
        // 重试是同一条执行记录的多次尝试，不产生新记录
        assertThat(attempts.get()).isEqualTo(3);
        assertThat(engine.store().countDuplicateEvents("t1")).isZero();
    }
}
