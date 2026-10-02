package com.github.diudiuseven.scheduledtaskenginejava.engine.policy;

/**
 * 同一任务上一次执行尚未结束时，新触发点的处理策略。
 */
public enum OverlapPolicy {
    /** 丢弃本次触发，记录为 SKIPPED，原因 overlap。 */
    SKIP,
    /** 进入队列等待，任一时刻仍最多一个有效执行。 */
    QUEUE
}
