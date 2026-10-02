package com.github.diudiuseven.scheduledtaskenginejava.engine.model;

/**
 * 一次触发点执行的生命周期状态。
 */
public enum ExecutionStatus {
    /** 已生成，等待调度（含排队与等待重试）。 */
    PENDING,
    /** 正在执行。 */
    RUNNING,
    /** 执行成功。 */
    SUCCESS,
    /** 执行失败且重试次数已耗尽。 */
    FAILED,
    /** 被跳过（补跑窗口外 / 重叠 / 限流拒绝 / 重复触发）。 */
    SKIPPED,
    /** 被取消。 */
    CANCELLED
}
