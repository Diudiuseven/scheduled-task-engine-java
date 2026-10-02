package com.github.diudiuseven.scheduledtaskenginejava.engine.trigger;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CronTriggerTest {

    private static final ZoneId BERLIN = ZoneId.of("Europe/Berlin");

    @Test
    void springGap_firePointShiftsToAfterGap() {
        CronTrigger trigger = CronTrigger.of("30 2 * * *", BERLIN); // 每天 02:30
        Instant after = ZonedDateTime.of(2024, 3, 30, 2, 30, 0, 0, BERLIN).toInstant();

        Instant next = trigger.nextFireAfter(after);

        // 03-31 02:30 不存在（02:00 跳变到 03:00），顺延到 03:00
        Instant expected = ZonedDateTime.of(2024, 3, 31, 3, 0, 0, 0, BERLIN).toInstant();
        assertEquals(expected, next);
    }

    @Test
    void fallOverlap_sameWallTimeFiresTwice() {
        CronTrigger trigger = CronTrigger.of("30 2 * * *", BERLIN);
        Instant after = ZonedDateTime.of(2024, 10, 26, 2, 30, 0, 0, BERLIN).toInstant();

        Instant f1 = trigger.nextFireAfter(after);
        Instant f2 = trigger.nextFireAfter(f1);
        Instant f3 = trigger.nextFireAfter(f2);

        // 10-27 02:30 出现两次（+02:00 与 +01:00）
        assertEquals(ZonedDateTime.of(2024, 10, 27, 2, 30, 0, 0, BERLIN).toInstant(), f1);
        assertEquals(ZonedDateTime.of(2024, 10, 27, 2, 30, 0, 0, BERLIN).withLaterOffsetAtOverlap().toInstant(), f2);
        // 次日 02:30（+01:00）
        assertEquals(ZonedDateTime.of(2024, 10, 28, 2, 30, 0, 0, BERLIN).toInstant(), f3);
    }

    @Test
    void firePointsBetween_dstWeek_hasDeterministicCount() {
        CronTrigger trigger = CronTrigger.of("30 2 * * *", BERLIN);
        Instant from = ZonedDateTime.of(2024, 3, 25, 0, 0, 0, 0, BERLIN).toInstant();
        Instant to = ZonedDateTime.of(2024, 4, 2, 0, 0, 0, 0, BERLIN).toInstant();

        List<Instant> points = trigger.firePointsBetween(from, to);

        // 3/25..4/1 共 8 天，每天一个触发点（gap 日顺延），共 8 个
        assertEquals(8, points.size());
        // 所有触发点的本地时间要么是 02:30，要么是 gap 顺延的 03:00
        for (Instant p : points) {
            ZonedDateTime z = p.atZone(BERLIN);
            boolean normal = z.getHour() == 2 && z.getMinute() == 30;
            boolean shifted = z.getHour() == 3 && z.getMinute() == 0
                    && z.toLocalDate().equals(LocalDate.of(2024, 3, 31));
            assertEquals(true, normal || shifted);
        }
    }

    @Test
    void intervalTrigger_isDstIndependent() {
        Instant start = ZonedDateTime.of(2024, 3, 30, 12, 0, 0, 0, BERLIN).toInstant();
        IntervalTrigger trigger = IntervalTrigger.of(start, Duration.ofDays(1), BERLIN);

        Instant f1 = trigger.nextFireAfter(start);
        Instant f2 = trigger.nextFireAfter(f1);

        // 间隔基于绝对瞬时：48 小时整，与 DST 无关
        assertEquals(start.plus(Duration.ofDays(1)), f1);
        assertEquals(start.plus(Duration.ofDays(2)), f2);
    }
}
