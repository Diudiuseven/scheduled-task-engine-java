package com.github.diudiuseven.scheduledtaskenginejava.engine.trigger;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

/**
 * 周期触发定义。给定一个时间点，返回其后的下一个触发点。
 * 实现必须携带时区，保证夏令时切换前后结果确定且可复现。
 */
public interface Trigger {

    /**
     * 计算 after（不含）之后的下一个触发点；不存在时返回空。
     */
    Optional<Instant> nextAfter(Instant after);

    /**
     * 该触发定义使用的时区。
     */
    ZoneId zone();
}
