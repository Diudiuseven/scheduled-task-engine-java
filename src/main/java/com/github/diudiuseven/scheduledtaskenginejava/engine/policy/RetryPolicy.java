package com.github.diudiuseven.scheduledtaskenginejava.engine.policy;

import java.time.Duration;

/**
 * 失败重试策略：最大尝试次数、初始间隔、退避倍数与间隔上限。
 */
public record RetryPolicy(int maxAttempts, Duration initialDelay, double backoffMultiplier, Duration maxDelay) {

    public static final RetryPolicy NO_RETRY = new RetryPolicy(1, Duration.ZERO, 1.0, Duration.ZERO);

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (initialDelay.isNegative()) {
            throw new IllegalArgumentException("initialDelay must be >= 0");
        }
        if (backoffMultiplier < 1.0) {
            throw new IllegalArgumentException("backoffMultiplier must be >= 1.0");
        }
        if (maxDelay.isNegative()) {
            throw new IllegalArgumentException("maxDelay must be >= 0");
        }
    }

    /**
     * 第 attempt 次失败后（attempt 从 1 开始）到下一次重试的等待时长。
     */
    public Duration delayAfterFailure(int attempt) {
        // initialDelay * multiplier^(attempt-1)，封顶 maxDelay（maxDelay 为 0 表示不封顶）。
        double factor = Math.pow(backoffMultiplier, Math.max(0, attempt - 1));
        double millis = initialDelay.toMillis() * factor;
        Duration delay = Duration.ofMillis((long) Math.min(millis, Long.MAX_VALUE / 2));
        if (!maxDelay.isZero() && delay.compareTo(maxDelay) > 0) {
            return maxDelay;
        }
        return delay;
    }

    /**
     * 该次失败后是否还有重试机会。
     */
    public boolean hasRetryLeft(int attempt) {
        return attempt < maxAttempts;
    }
}
