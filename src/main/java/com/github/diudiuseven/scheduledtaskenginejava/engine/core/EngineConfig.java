package com.github.diudiuseven.scheduledtaskenginejava.engine.core;

import java.time.Duration;

/**
 * 引擎级配置：调度 tick 间隔、全局触发速率上限、全局并发上限。
 */
public record EngineConfig(Duration tickInterval, double maxTriggersPerSecond, int maxGlobalConcurrency) {

    public static EngineConfig defaults() {
        return new EngineConfig(Duration.ofMillis(100), 100.0, 16);
    }

    public EngineConfig {
        if (tickInterval.isZero() || tickInterval.isNegative()) {
            throw new IllegalArgumentException("tickInterval must be positive");
        }
        if (maxTriggersPerSecond <= 0) {
            throw new IllegalArgumentException("maxTriggersPerSecond must be > 0");
        }
        if (maxGlobalConcurrency < 1) {
            throw new IllegalArgumentException("maxGlobalConcurrency must be >= 1");
        }
    }
}
