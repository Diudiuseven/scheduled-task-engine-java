package com.github.diudiuseven.scheduledtaskenginejava.engine.model;

import java.time.Duration;

/**
 * 停机/长时间停止后的补跑策略。
 *
 * @param enabled 是否启用补跑
 * @param window  补跑窗口：(now - window, now] 内错过的触发点补算，更早的记为跳过
 */
public record CatchUpPolicy(boolean enabled, Duration window) {

    public static CatchUpPolicy of(Duration window) {
        return new CatchUpPolicy(true, window);
    }

    public static CatchUpPolicy disabled() {
        return new CatchUpPolicy(false, Duration.ZERO);
    }
}
