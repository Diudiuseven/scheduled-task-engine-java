package com.github.diudiuseven.scheduledtaskenginejava.engine.trigger;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

/**
 * 周期触发定义。触发点以带时区的方式计算，时区语义由具体实现保证。
 */
public sealed interface TriggerDefinition permits IntervalTrigger, CronTrigger {

    /** 触发定义所使用的时区。 */
    ZoneId zone();

    /** 返回严格晚于 {@code after} 的下一个触发时刻（UTC 瞬时）。 */
    Instant nextFireAfter(Instant after);

    /** 返回 (from, to] 区间内的全部触发时刻，按时间升序。 */
    List<Instant> firePointsBetween(Instant from, Instant to);
}
