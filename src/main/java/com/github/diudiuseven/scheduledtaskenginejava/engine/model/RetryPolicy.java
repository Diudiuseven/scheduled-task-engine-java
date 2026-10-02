package com.github.diudiuseven.scheduledtaskenginejava.engine.model;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 失败重试与退避策略。
 *
 * @param maxAttempts 最大执行次数（含首次）
 * @param backoffBase 首次重试退避基数
 * @param backoffMax  退避上限
 * @param factor      退避指数因子
 * @param jitterRatio 随机抖动比例（0~1），避免重试风暴
 */
public record RetryPolicy(int maxAttempts,
                          Duration backoffBase,
                          Duration backoffMax,
                          double factor,
                          double jitterRatio) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (factor < 1.0) {
            throw new IllegalArgumentException("factor must be >= 1");
        }
        if (jitterRatio < 0 || jitterRatio > 1) {
            throw new IllegalArgumentException("jitterRatio must be in [0,1]");
        }
    }

    public static RetryPolicy of(int maxAttempts, Duration backoffBase, Duration backoffMax,
                                 double factor, double jitterRatio) {
        return new RetryPolicy(maxAttempts, backoffBase, backoffMax, factor, jitterRatio);
    }

    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 1.0, 0.0);
    }

    /** 第 attempt 次失败后（attempt 从 1 开始）的退避时长（含随机抖动）。 */
    public Duration backoffAfter(int attempt) {
        double base = backoffBase.toNanos();
        double raw = base * Math.pow(factor, attempt - 1.0);
        double capped = Math.min(raw, backoffMax.toNanos());
        double jitter = 1.0 + (ThreadLocalRandom.current().nextDouble() * 2.0 - 1.0) * jitterRatio;
        return Duration.ofNanos(Math.max(0, (long) (capped * jitter)));
    }

    /** 退避时长（不含抖动），用于确定性断言。 */
    public Duration backoffAfterNoJitter(int attempt) {
        double raw = backoffBase.toNanos() * Math.pow(factor, attempt - 1.0);
        return Duration.ofNanos((long) Math.min(raw, backoffMax.toNanos()));
    }
}
