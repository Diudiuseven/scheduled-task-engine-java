package com.github.diudiuseven.scheduledtaskenginejava.engine.trigger;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * 日历（cron）表达式触发，携带时区。
 * 支持 5 字段：分 时 日 月 周（秒恒为 0）。
 *
 * 夏令时语义（确定且可复现）：
 * - 春季跳变（gap）：本地时间在跳变窗口内不存在，触发点顺延为跳变结束后的第一个有效时刻；
 * - 秋季重叠（overlap）：同一本地时间对应两个偏移，两个瞬时各触发一次。
 * 因此春秋切换日的触发次数由表达式唯一确定，详见 README。
 */
public final class CronTrigger implements TriggerDefinition {

    private static final Map<String, Integer> MONTH_NAMES = Map.ofEntries(
            Map.entry("JAN", 1), Map.entry("FEB", 2), Map.entry("MAR", 3), Map.entry("APR", 4),
            Map.entry("MAY", 5), Map.entry("JUN", 6), Map.entry("JUL", 7), Map.entry("AUG", 8),
            Map.entry("SEP", 9), Map.entry("OCT", 10), Map.entry("NOV", 11), Map.entry("DEC", 12));
    private static final Map<String, Integer> DOW_NAMES = Map.ofEntries(
            Map.entry("SUN", 0), Map.entry("MON", 1), Map.entry("TUE", 2), Map.entry("WED", 3),
            Map.entry("THU", 4), Map.entry("FRI", 5), Map.entry("SAT", 6));

    private final String expression;
    private final ZoneId zone;
    private final BitSet minuteBits = new BitSet(60);
    private final BitSet hourBits = new BitSet(24);
    private final BitSet domBits = new BitSet(31);
    private final BitSet monthBits = new BitSet(12);
    private final BitSet dowBits = new BitSet(7);
    private final boolean domRestricted;
    private final boolean dowRestricted;

    public CronTrigger(String expression, ZoneId zone) {
        this.expression = expression;
        this.zone = zone;
        String[] fields = expression.trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException("Cron expression must have 5 fields (minute hour dom month dow): " + expression);
        }
        parseField(fields[0], 0, 59, null, minuteBits);
        parseField(fields[1], 0, 23, null, hourBits);
        domRestricted = parseField(fields[2], 1, 31, null, domBits);
        parseField(fields[3], 1, 12, MONTH_NAMES, monthBits);
        dowRestricted = parseDowField(fields[4]);
    }

    public static CronTrigger of(String expression, ZoneId zone) {
        return new CronTrigger(expression, zone);
    }

    public String expression() {
        return expression;
    }

    @Override
    public ZoneId zone() {
        return zone;
    }

    @Override
    public Instant nextFireAfter(Instant after) {
        // 从 after 所在分钟（截断秒）开始：重叠时同一本地时间对应两个偏移瞬时，
        // 必须在推进本地时间之前先把同一本地时间的所有瞬时都判断一遍。
        LocalDateTime candidate = LocalDateTime.ofInstant(after, zone).withSecond(0).withNano(0);
        while (true) {
            if (matches(candidate)) {
                for (Instant inst : resolveToInstants(candidate)) {
                    if (inst.isAfter(after)) {
                        return inst;
                    }
                }
            }
            candidate = nextMatchingLocalDateTime(candidate);
        }
    }

    @Override
    public List<Instant> firePointsBetween(Instant from, Instant to) {
        List<Instant> points = new ArrayList<>();
        Instant t = nextFireAfter(from);
        while (!t.isAfter(to)) {
            points.add(t);
            t = nextFireAfter(t);
        }
        return points;
    }

    /**
     * 将本地时间解析为一个或多个瞬时：
     * gap（春）→ 跳变后的第一个有效瞬时；overlap（秋）→ 两个偏移各一个瞬时。
     */
    private List<Instant> resolveToInstants(LocalDateTime ldt) {
        ZoneRules rules = zone.getRules();
        List<ZoneOffset> offsets = rules.getValidOffsets(ldt);
        if (offsets.isEmpty()) {
            ZoneOffsetTransition transition = rules.getTransition(ldt);
            LocalDateTime afterGap = transition.getDateTimeAfter();
            return List.of(afterGap.atZone(zone).toInstant());
        }
        List<Instant> instants = new ArrayList<>(offsets.size());
        for (ZoneOffset offset : offsets) {
            instants.add(ldt.atOffset(offset).toInstant());
        }
        return instants;
    }

    /** 本地时间是否匹配 cron 表达式（秒恒为 0）。 */
    private boolean matches(LocalDateTime ldt) {
        return ldt.getSecond() == 0
                && minuteBits.get(ldt.getMinute())
                && hourBits.get(ldt.getHour())
                && monthBits.get(ldt.getMonthValue())
                && dayMatches(ldt.toLocalDate());
    }

    /** 返回严格晚于 {@code from} 的第一个匹配本地时间（秒以下截断为 0）。 */
    private LocalDateTime nextMatchingLocalDateTime(LocalDateTime from) {
        LocalDateTime t = from.withSecond(0).withNano(0).plusMinutes(1);
        for (int guard = 0; guard < 60 * 24 * 366 * 8; guard++) {
            LocalDate date = t.toLocalDate();
            if (monthBits.get(date.getMonthValue()) && dayMatches(date)) {
                LocalTime lt = firstMatchingTimeOnOrAfter(t.toLocalTime());
                if (lt != null) {
                    return LocalDateTime.of(date, lt);
                }
            }
            t = date.plusDays(1).atStartOfDay();
        }
        throw new IllegalStateException("No matching cron fire time within 8 years for expression: " + expression);
    }

    private LocalTime firstMatchingTimeOnOrAfter(LocalTime lowerBound) {
        for (int h = lowerBound.getHour(); h <= 23; h++) {
            if (!hourBits.get(h)) {
                continue;
            }
            int minuteFrom = (h == lowerBound.getHour()) ? lowerBound.getMinute() : 0;
            for (int m = minuteFrom; m <= 59; m++) {
                if (minuteBits.get(m)) {
                    return LocalTime.of(h, m);
                }
            }
        }
        return null;
    }

    private boolean dayMatches(LocalDate date) {
        boolean domOk = domBits.get(date.getDayOfMonth());
        int cronDow = date.getDayOfWeek().getValue() % 7; // MON=1..SUN=7 → SUN=0
        boolean dowOk = dowBits.get(cronDow);
        if (domRestricted && dowRestricted) {
            return domOk || dowOk; // 标准 cron：日与周同时限制时取或
        }
        if (domRestricted) {
            return domOk;
        }
        if (dowRestricted) {
            return dowOk;
        }
        return true;
    }

    /** 解析普通字段，返回该字段是否被显式限制（非 * / ?）。 */
    private static boolean parseField(String field, int min, int max,
                                     Map<String, Integer> names, BitSet target) {
        boolean restricted = !(field.equals("*") || field.equals("?"));
        for (String part : field.split(",")) {
            int step = 1;
            String range = part;
            int slash = part.indexOf('/');
            if (slash >= 0) {
                range = part.substring(0, slash);
                step = parseIntValue(part.substring(slash + 1), names);
                if (step <= 0) {
                    throw new IllegalArgumentException("Invalid step in cron field: " + field);
                }
            }
            int lo;
            int hi;
            if (range.equals("*") || range.equals("?")) {
                lo = min;
                hi = max;
            } else if (range.indexOf('-') >= 0) {
                String[] rr = range.split("-");
                lo = parseIntValue(rr[0], names);
                hi = parseIntValue(rr[1], names);
            } else {
                lo = parseIntValue(range, names);
                hi = (slash >= 0) ? max : lo;
            }
            if (lo < min || hi > max || lo > hi) {
                throw new IllegalArgumentException("Cron value out of range [" + min + "," + max + "]: " + field);
            }
            for (int v = lo; v <= hi; v += step) {
                target.set(v);
            }
        }
        return restricted;
    }

    private boolean parseDowField(String field) {
        boolean restricted = !(field.equals("*") || field.equals("?"));
        BitSet raw = new BitSet(7);
        parseField(field, 0, 7, DOW_NAMES, raw);
        for (int v = raw.nextSetBit(0); v >= 0; v = raw.nextSetBit(v + 1)) {
            dowBits.set(v % 7); // 7 与 0 都表示周日
        }
        return restricted;
    }

    private static int parseIntValue(String token, Map<String, Integer> names) {
        String t = token.trim().toUpperCase();
        if (names != null && names.containsKey(t)) {
            return names.get(t);
        }
        try {
            return Integer.parseInt(t);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Unparseable cron token: " + token);
        }
    }

    @Override
    public String toString() {
        return "CronTrigger[" + expression + " @ " + zone + "]";
    }
}
