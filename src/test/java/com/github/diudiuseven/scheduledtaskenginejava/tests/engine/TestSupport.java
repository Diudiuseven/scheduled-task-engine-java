package com.github.diudiuseven.scheduledtaskenginejava.tests.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.function.BooleanSupplier;

/**
 * 测试支撑：可变时钟与轮询等待。
 */
final class TestSupport {

    private TestSupport() {
    }

    /** 可手动推进的时钟，用于模拟停机与补跑。 */
    static final class MutableClock extends Clock {
        private Instant now;
        private final ZoneId zone;

        MutableClock(Instant now) {
            this(now, ZoneId.of("UTC"));
        }

        MutableClock(Instant now, ZoneId zone) {
            this.now = now;
            this.zone = zone;
        }

        synchronized void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return new MutableClock(now, zone);
        }

        @Override
        public synchronized Instant instant() {
            return now;
        }
    }

    /** 轮询等待条件成立，超时则断言失败。 */
    static void await(BooleanSupplier condition, Duration timeout, String description) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while awaiting: " + description);
            }
        }
        throw new AssertionError("timed out awaiting: " + description);
    }

    static void await(BooleanSupplier condition, String description) {
        await(condition, Duration.ofSeconds(10), description);
    }
}
