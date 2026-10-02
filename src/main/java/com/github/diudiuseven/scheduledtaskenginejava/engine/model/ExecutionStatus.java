package com.github.diudiuseven.scheduledtaskenginejava.engine.model;

/**
 * 触发执行记录的生命周期状态。
 */
public enum ExecutionStatus {
    /** 已创建，等待入队/调度 */
    PENDING,
    /** 已进入延迟队列，等待执行 */
    QUEUED,
    /** 执行中 */
    RUNNING,
    /** 执行成功（终态） */
    SUCCEEDED,
    /** 重试次数用尽，执行失败（终态） */
    FAILED,
    /** 被跳过（终态），原因见 SkipReason */
    SKIPPED,
    /** 失败后等待退避重试 */
    RETRY_WAIT,
    /** 被用户或恢复流程取消（终态） */
    CANCELLED;

    /** 终态：不会再被调度执行。 */
    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == SKIPPED || this == CANCELLED;
    }
}
