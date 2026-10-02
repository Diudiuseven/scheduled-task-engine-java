package com.github.diudiuseven.scheduledtaskenginejava.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.CatchUpPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionRecord;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RetryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.SkipReason;
import com.github.diudiuseven.scheduledtaskenginejava.engine.support.Engines;
import com.github.diudiuseven.scheduledtaskenginejava.engine.support.MutableClock;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 停机/长时间停止后的补跑：窗口内补算执行，窗口外记录跳过原因。
 */
class CatchUpTest {

    @TempDir
    Path dbDir;

    private ScheduledTaskEngine engine;
    private MutableClock clock;
    private final Map<Instant, AtomicInteger> fireCounts = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"), ZoneOffset.UTC);
        engine = Engines.fileBacked(dbDir, clock, Engines.fastProperties());
    }

    @AfterEach
    void tearDown() {
        engine.stop();
    }

    @Test
    void missedPoints_withinWindowAreReplayed_beyondWindowSkipped() {
        Instant start = clock.instant();
        TaskDefinition task = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofSeconds(10), ZoneOffset.UTC),
                        ctx -> fireCounts.computeIfAbsent(ctx.getFireTime(), k -> new AtomicInteger()).incrementAndGet())
                .catchUpPolicy(CatchUpPolicy.of(Duration.ofSeconds(30)))
                .overlapPolicy(OverlapPolicy.QUEUE)
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
        engine.register(task);
        engine.start();

        // 模拟停机 2 分钟
        clock.advance(Duration.ofMinutes(2));

        await().atMost(java.time.Duration.ofSeconds(10)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t1");
            long succeeded = records.stream().filter(r -> r.getStatus() == ExecutionStatus.SUCCEEDED).count();
            long skipped = records.stream().filter(r -> r.getStatus() == ExecutionStatus.SKIPPED).count();
            assertThat(succeeded).isEqualTo(4); // 窗口内：90s/100s/110s/120s
            assertThat(skipped).isEqualTo(8);    // 窗口外：10s..80s
        });

        List<ExecutionRecord> records = engine.executionsOf("t1");
        // 每个触发点恰好一条记录，不丢不重
        assertThat(records).hasSize(12);
        assertThat(records).allSatisfy(r -> assertThat(r.getFireTime()).isNotNull());
        // 跳过原因全部记录
        assertThat(records.stream()
                .filter(r -> r.getStatus() == ExecutionStatus.SKIPPED)
                .map(ExecutionRecord::getSkipReason))
                .containsOnly(SkipReason.CATCHUP_WINDOW);
        // 窗口内的触发点确实被执行
        assertThat(fireCounts).containsOnlyKeys(
                start.plusSeconds(90), start.plusSeconds(100),
                start.plusSeconds(110), start.plusSeconds(120));
    }

    @Test
    void catchUpDisabled_allMissedPointsExecuted() {
        Instant start = clock.instant();
        TaskDefinition task = TaskDefinition.builder("t2",
                        IntervalTrigger.of(start, Duration.ofSeconds(10), ZoneOffset.UTC),
                        ctx -> fireCounts.computeIfAbsent(ctx.getFireTime(), k -> new AtomicInteger()).incrementAndGet())
                .catchUpPolicy(CatchUpPolicy.disabled())
                .overlapPolicy(OverlapPolicy.QUEUE)
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
        engine.register(task);
        engine.start();

        clock.advance(Duration.ofMinutes(1));

        await().atMost(java.time.Duration.ofSeconds(10)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t2");
            assertThat(records).hasSize(6);
            assertThat(records).allSatisfy(r ->
                    assertThat(r.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED));
        });
    }
}
