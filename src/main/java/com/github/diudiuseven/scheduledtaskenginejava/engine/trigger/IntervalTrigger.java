package com.github.diudiuseven.scheduledtaskenginejava.engine.trigger;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 固定间隔触发：从起始时刻起，每隔 {@code interval} 触发一次。
 * 间隔基于绝对瞬时（Duration），不受夏令时切换影响。
 */
public final class IntervalTrigger implements TriggerDefinition {

    private final Instant start;
    private final Duration interval;
    private final ZoneId zone;

    public IntervalTrigger(Instant start, Duration interval, ZoneId zone) {
        this.start = Objects.requireNonNull(start, "start");
        this.interval = Objects.requireNonNull(interval, "interval");
        this.zone = Objects.requireNonNull(zone, "zone");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
    }

    public static IntervalTrigger of(Instant start, Duration interval, ZoneId zone) {
        return new IntervalTrigger(start, interval, zone);
    }

    @Override
    public ZoneId zone() {
        return zone;
    }

    public Instant start() {
        return start;
    }

    public Duration interval() {
        return interval;
    }

    @Override
    public Instant nextFireAfter(Instant after) {
        long intervalNanos = interval.toNanos();
        long startNanos = start.getEpochSecond() * 1_000_000_000L + start.getNano();
        long afterNanos = after.getEpochSecond() * 1_000_000_000L + after.getNano();
        long k = Math.floorDiv(afterNanos - startNanos, intervalNanos) + 1;
        long nextNanos = startNanos + k * intervalNanos;
        return Instant.ofEpochSecond(nextNanos / 1_000_000_000L, nextNanos % 1_000_000_000L);
    }

    @Override
    public List<Instant> firePointsBetween(Instant from, Instant to) {
        List<Instant> points = new ArrayList<>();
        Instant t = nextFireAfter(from);
        while (!t.isAfter(to)) {
            points.add(t);
            t = t.plus(interval);
        }
        return points;
    }
}
