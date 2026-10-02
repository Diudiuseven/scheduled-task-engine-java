package com.github.diudiuseven.scheduledtaskenginejava.engine.model;

/**
 * 触发点被跳过/拒绝的原因。
 */
public enum SkipReason {
    /** 超出补跑窗口，不再补算 */
    CATCHUP_WINDOW,
    /** 任务上一次执行尚未结束，按重叠策略跳过 */
    OVERLAP,
    /** 同一触发点被重复调度，去重 */
    DUPLICATE,
    /** 触发速率超限且等待超时 */
    RATE_LIMITED,
    /** 单任务并发度超限且等待超时 */
    CONCURRENCY_LIMITED,
    /** 执行队列已满 */
    QUEUE_FULL,
    /** 重启后在途任务无法恢复且按策略置为失败 */
    RECOVERY_FAILED,
    /** 用户主动取消 */
    CANCELLED_BY_USER
}
