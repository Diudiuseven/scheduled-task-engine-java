package com.github.diudiuseven.scheduledtaskenginejava.engine;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 执行耗时超过周期：overlap=SKIP 时同一时刻最多一个有效执行，错过的触发点记录跳过。
 */
class OverlapTest {

    @TempDir
    Path dbDir;

    private ScheduledTaskEngine engine;
    private final List<long[]> intervals = new ArrayList<>();
    private final CountDownLatch blocker = new CountDownLatch(1);

    @BeforeEach
    void setUp() {
        engine = Engines.fileBacked(dbDir, java.time.Clock.systemUTC(), Engines.fastProperties());
    }

    @AfterEach
    void tearDown() {
        engine.stop();
    }

    @Test
    void longExecution_overlapPolicySkip_skipsMissedTicks() throws Exception {
        Instant start = Instant.now();
        TaskDefinition task = TaskDefinition.builder("t1",
                        IntervalTrigger.of(start, Duration.ofMillis(50), ZoneOffset.UTC),
                        ctx -> {
                            long s = System.nanoTime();
                            try {
                                blocker.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            long e = System.nanoTime();
                            synchronized (intervals) {
                                intervals.add(new long[]{s, e});
                            }
                        })
                .overlapPolicy(OverlapPolicy.SKIP)
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(start)
                .build();
        engine.register(task);
        engine.start();

        // 等待第一次执行进入 RUNNING
        await().atMost(Duration.ofSeconds(5)).until(() ->
                engine.executionsOf("t1").stream()
                        .anyMatch(r -> r.getStatus() == ExecutionStatus.RUNNING));

        // 阻塞期间多个触发点被跳过
        Thread.sleep(250);
        blocker.countDown();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t1");
            long running = records.stream().filter(r -> !r.getStatus().isTerminal()).count();
            assertThat(running).isZero();
        });

        List<ExecutionRecord> records = engine.executionsOf("t1");
        long succeeded = records.stream().filter(r -> r.getStatus() == ExecutionStatus.SUCCEEDED).count();
        long overlapSkips = records.stream()
                .filter(r -> r.getStatus() == ExecutionStatus.SKIPPED)
                .filter(r -> r.getSkipReason() == SkipReason.OVERLAP)
                .count();
        assertThat(succeeded).isGreaterThanOrEqualTo(1);
        assertThat(overlapSkips).isGreaterThanOrEqualTo(3);
        // 每个触发点唯一
        assertThat(records).extracting(ExecutionRecord::getFireTime).doesNotHaveDuplicates();

        // 执行区间互不重叠
        synchronized (intervals) {
            assertThat(intervals).isNotEmpty();
            for (int i = 1; i < intervals.size(); i++) {
                assertThat(intervals.get(i)[0]).isGreaterThanOrEqualTo(intervals.get(i - 1)[1]);
            }
        }
    }
}
