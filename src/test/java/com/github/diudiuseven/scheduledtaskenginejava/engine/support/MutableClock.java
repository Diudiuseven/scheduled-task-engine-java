package com.github.diudiuseven.scheduledtaskenginejava.engine.support;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * 可手动推进的时钟，用于确定性测试补跑/恢复等时间相关语义。
 */
public class MutableClock extends Clock {

    private final ZoneId zone;
    private Instant instant;

    public MutableClock(Instant instant) {
        this(instant, ZoneOffset.UTC);
    }

    public MutableClock(Instant instant, ZoneId zone) {
        this.instant = instant;
        this.zone = zone;
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new MutableClock(instant, zone);
    }

    @Override
    public Instant instant() {
        return instant;
    }

    public void advance(Duration duration) {
        this.instant = this.instant.plus(duration);
    }

    public void set(Instant instant) {
        this.instant = instant;
    }
}
