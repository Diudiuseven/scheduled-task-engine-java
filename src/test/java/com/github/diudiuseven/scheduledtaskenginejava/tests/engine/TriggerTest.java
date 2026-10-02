package com.github.diudiuseven.scheduledtaskenginejava.tests.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.CronTrigger;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.FixedIntervalTrigger;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class TriggerTest {

    @Test
    void fixedIntervalAdvancesFromAnchor() {
        Instant anchor = Instant.parse("2024-01-01T00:00:00Z");
        FixedIntervalTrigger trigger = new FixedIntervalTrigger(anchor, Duration.ofMinutes(10), ZoneId.of("UTC"));

        assertEquals(Optional.of(anchor), trigger.nextAfter(anchor.minusSeconds(1)));
        assertEquals(Optional.of(anchor.plusSeconds(600)), trigger.nextAfter(anchor));
        assertEquals(Optional.of(anchor.plusSeconds(1200)), trigger.nextAfter(anchor.plusSeconds(600)));
        // 非对齐时间点 -> 下一个对齐触发点
        assertEquals(Optional.of(anchor.plusSeconds(1200)), trigger.nextAfter(anchor.plusSeconds(601)));
    }

    @Test
    void cronFiresAtExpectedLocalTimes() {
        CronTrigger trigger = new CronTrigger("0 30 9 * * *", ZoneId.of("Asia/Shanghai"));
        Optional<Instant> next = trigger.nextAfter(Instant.parse("2024-06-01T00:00:00Z"));
        // 上海 09:30 = UTC 01:30
        assertEquals(Optional.of(Instant.parse("2024-06-01T01:30:00Z")), next);
    }

    @Test
    void cronAcrossSpringForwardIsDeterministic() {
        // 美国东部 2024-03-10 02:00 -> 03:00 春进，本地 02:30 不存在
        ZoneId ny = ZoneId.of("America/New_York");
        CronTrigger trigger = new CronTrigger("0 30 2 * * *", ny);
        Instant from = Instant.parse("2024-03-09T00:00:00Z");

        List<Instant> fires = new ArrayList<>();
        Instant cursor = from;
        for (int i = 0; i < 3; i++) {
            Optional<Instant> next = trigger.nextAfter(cursor);
            assertTrue(next.isPresent());
            fires.add(next.get());
            cursor = next.get();
        }
        // 3-09 02:30 EST = 07:30Z；3-10 本地 02:30 不存在，顺延到 03:30 EDT = 07:30Z；3-11 02:30 EDT = 06:30Z
        assertEquals(List.of(
                Instant.parse("2024-03-09T07:30:00Z"),
                Instant.parse("2024-03-10T07:30:00Z"),
                Instant.parse("2024-03-11T06:30:00Z")), fires);

        // 可复现：同样的输入再次计算结果一致
        List<Instant> replay = new ArrayList<>();
        cursor = from;
        for (int i = 0; i < 3; i++) {
            replay.add(trigger.nextAfter(cursor).orElseThrow());
            cursor = replay.get(replay.size() - 1);
        }
        assertEquals(fires, replay);
    }

    @Test
    void cronAcrossFallBackFiresOncePerDay() {
        // 美国东部 2024-11-03 02:00 -> 01:00 回拨，本地 01:30 出现两次，每天只触发一次
        ZoneId ny = ZoneId.of("America/New_York");
        CronTrigger trigger = new CronTrigger("0 30 1 * * *", ny);
        Instant from = Instant.parse("2024-11-02T00:00:00Z");

        List<Instant> fires = new ArrayList<>();
        Instant cursor = from;
        for (int i = 0; i < 3; i++) {
            Optional<Instant> next = trigger.nextAfter(cursor);
            assertTrue(next.isPresent());
            fires.add(next.get());
            cursor = next.get();
        }
        // 11-02 01:30 EDT = 05:30Z；11-03 第一次 01:30 EDT = 05:30Z（只取一次）；11-04 01:30 EST = 06:30Z
        assertEquals(List.of(
                Instant.parse("2024-11-02T05:30:00Z"),
                Instant.parse("2024-11-03T05:30:00Z"),
                Instant.parse("2024-11-04T06:30:00Z")), fires);
    }

    @Test
    void fixedIntervalUnaffectedByDst() {
        // 固定间隔按绝对时间推进，跨过夏令时切换点间隔不变
        Instant anchor = Instant.parse("2024-03-10T06:00:00Z");
        FixedIntervalTrigger trigger = new FixedIntervalTrigger(anchor, Duration.ofHours(1), ZoneId.of("America/New_York"));
        assertEquals(Optional.of(Instant.parse("2024-03-10T07:00:00Z")), trigger.nextAfter(anchor));
        assertEquals(Optional.of(Instant.parse("2024-03-10T08:00:00Z")),
                trigger.nextAfter(Instant.parse("2024-03-10T07:00:00Z")));
    }
}
