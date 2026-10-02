package com.github.diudiuseven.scheduledtaskenginejava.engine.policy;

import java.time.Duration;

/**
 * 补跑策略：停机恢复后，补算窗口内的错过触发点会被补跑，
 * 超出窗口的触发点记录为 SKIPPED，原因 out-of-misfire-window。
 */
public record MisfirePolicy(Duration catchUpWindow) {

    public static final MisfirePolicy DEFAULT = new MisfirePolicy(Duration.ofHours(1));

    public MisfirePolicy {
        if (catchUpWindow.isNegative()) {
            throw new IllegalArgumentException("catchUpWindow must be >= 0");
        }
    }

    /**
     * 判断错过的触发点 fireTime 相对当前时间 now 是否仍在补跑窗口内。
     */
    public boolean withinWindow(java.time.Instant fireTime, java.time.Instant now) {
        // 触发点落在 [now - catchUpWindow, now] 内视为可补跑。
        return !fireTime.isBefore(now.minus(catchUpWindow));
    }
}
