package com.github.diudiuseven.scheduledtaskenginejava.engine;

import java.util.concurrent.TimeUnit;

/**
 * 令牌桶限速：通过串行化“下一次允许时刻”平滑触发速率。
 */
final class RateLimiter {

    private final double permitsPerSecond;
    private long nextFreeNanos;

    RateLimiter(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
        this.nextFreeNanos = System.nanoTime();
    }

    /**
     * 在超时时间内获取一个许可。
     *
     * @return true=获取成功；false=超时
     */
    synchronized boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        if (permitsPerSecond <= 0) {
            return true;
        }
        long now = System.nanoTime();
        long waitNanos = nextFreeNanos - now;
        if (waitNanos > unit.toNanos(timeout)) {
            return false;
        }
        if (waitNanos > 0) {
            TimeUnit.NANOSECONDS.sleep(waitNanos);
        }
        long intervalNanos = (long) (1_000_000_000L / permitsPerSecond);
        nextFreeNanos = Math.max(nextFreeNanos, System.nanoTime()) + intervalNanos;
        return true;
    }
}
