package com.github.diudiuseven.scheduledtaskenginejava.engine.trigger;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;

/**
 * 固定间隔触发：从锚点开始每隔 interval 触发一次。
 * 间隔按绝对时间（Instant）推进，不受夏令时影响。
 */
public class FixedIntervalTrigger implements Trigger {

    private final Instant anchor;
    private final Duration interval;
    private final ZoneId zone;

    public FixedIntervalTrigger(Instant anchor, Duration interval, ZoneId zone) {
        this.anchor = Objects.requireNonNull(anchor, "anchor");
        this.interval = Objects.requireNonNull(interval, "interval");
        this.zone = Objects.requireNonNull(zone, "zone");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
    }

    @Override
    public Optional<Instant> nextAfter(Instant after) {
        // 触发点序列为 anchor + k * interval（k >= 0），按绝对时间推进。
        if (after.isBefore(anchor)) {
            return Optional.of(anchor);
        }
        long intervalMillis = interval.toMillis();
        long elapsedMillis = after.toEpochMilli() - anchor.toEpochMilli();
        long k = elapsedMillis / intervalMillis + 1;
        return Optional.of(anchor.plusMillis(k * intervalMillis));
    }

    @Override
    public ZoneId zone() {
        return zone;
    }

    public Instant anchor() {
        return anchor;
    }

    public Duration interval() {
        return interval;
    }
}
