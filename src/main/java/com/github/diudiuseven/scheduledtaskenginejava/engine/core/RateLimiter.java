package com.github.diudiuseven.scheduledtaskenginejava.engine.core;

/**
 * 简单令牌桶限流器，限制全局触发速率。
 */
public class RateLimiter {

    private final double permitsPerSecond;
    private double permits;
    private long lastRefillNanos;

    public RateLimiter(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
        this.permits = permitsPerSecond;
        this.lastRefillNanos = System.nanoTime();
    }

    /**
     * 尝试获取一个许可，不阻塞。
     */
    public synchronized boolean tryAcquire() {
        refill();
        if (permits >= 1.0) {
            permits -= 1.0;
            return true;
        }
        return false;
    }

    private void refill() {
        long now = System.nanoTime();
        double elapsedSeconds = (now - lastRefillNanos) / 1_000_000_000.0;
        permits = Math.min(permitsPerSecond, permits + elapsedSeconds * permitsPerSecond);
        lastRefillNanos = now;
    }
}
