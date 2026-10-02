package com.github.diudiuseven.scheduledtaskenginejava.engine.trigger;

import org.springframework.scheduling.support.CronExpression;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;

/**
 * 日历表达式（cron）触发，基于 Spring CronExpression，携带时区。
 * 使用 ZonedDateTime 推进，夏令时切换前后触发次数确定且可复现。
 */
public class CronTrigger implements Trigger {

    private final CronExpression expression;
    private final ZoneId zone;
    private final String source;

    public CronTrigger(String cron, ZoneId zone) {
        this.source = Objects.requireNonNull(cron, "cron");
        this.zone = Objects.requireNonNull(zone, "zone");
        this.expression = CronExpression.parse(cron);
    }

    @Override
    public Optional<Instant> nextAfter(Instant after) {
        // 在“本地时间空间”中寻找下一个触发时刻，再解析回时区：
        // - 春进跳空：不存在的本地时间顺延 gap 长度（02:30 -> 03:30），当天不丢触发；
        // - 秋退回拨：重复的本地时刻只取第一次出现，当天不重复触发。
        // 因此夏令时切换前后同一时刻的触发次数确定且可复现。
        java.time.LocalDateTime nextLocal = expression.next(after.atZone(zone).toLocalDateTime());
        if (nextLocal == null) {
            return Optional.empty();
        }
        return Optional.of(ZonedDateTime.ofLocal(nextLocal, zone, null).toInstant());
    }

    @Override
    public ZoneId zone() {
        return zone;
    }

    public String source() {
        return source;
    }
}
