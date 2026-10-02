package com.github.diudiuseven.scheduledtaskenginejava.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.CatchUpPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionRecord;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RetryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.support.Engines;
import com.github.diudiuseven.scheduledtaskenginejava.engine.support.MutableClock;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.CronTrigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 引擎集成：cron 触发在夏令时切换日的触发点确定可复现。
 */
class DstEngineTest {

    private static final ZoneId BERLIN = ZoneId.of("Europe/Berlin");

    @TempDir
    Path dbDir;

    private ScheduledTaskEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Test
    void cronAcrossSpringGap_firesAtShiftedTime() {
        MutableClock clock = new MutableClock(
                ZonedDateTime.of(2024, 3, 25, 0, 0, 0, 0, BERLIN).toInstant());
        engine = Engines.fileBacked(dbDir, clock, Engines.fastProperties());

        TaskDefinition task = TaskDefinition.builder("t1",
                        CronTrigger.of("30 2 * * *", BERLIN),
                        ctx -> { })
                .overlapPolicy(OverlapPolicy.QUEUE)
                .catchUpPolicy(CatchUpPolicy.of(Duration.ofDays(30)))
                .retryPolicy(RetryPolicy.noRetry())
                .startTime(clock.instant())
                .build();
        engine.register(task);
        engine.start();

        clock.set(ZonedDateTime.of(2024, 4, 2, 0, 0, 0, 0, BERLIN).toInstant());

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<ExecutionRecord> records = engine.executionsOf("t1");
            assertThat(records).hasSize(8); // 3/25..4/1 共 8 天
            assertThat(records).allSatisfy(r ->
                    assertThat(r.getStatus()).isEqualTo(ExecutionStatus.SUCCEEDED));
        });

        List<ExecutionRecord> records = engine.executionsOf("t1");
        // gap 日（3/31）的触发点顺延到 03:00
        Instant shifted = ZonedDateTime.of(2024, 3, 31, 3, 0, 0, 0, BERLIN).toInstant();
        assertThat(records).extracting(ExecutionRecord::getFireTime).contains(shifted);
    }
}
