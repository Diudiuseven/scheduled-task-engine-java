package com.github.diudiuseven.scheduledtaskenginejava.engine.policy;

/**
 * 超出速率或并发限制时的处理策略。
 */
public enum RejectPolicy {
    /** 排队等待许可。 */
    QUEUE,
    /** 直接拒绝，记录为 SKIPPED，原因 rate-limited / concurrency-limited。 */
    REJECT
}
