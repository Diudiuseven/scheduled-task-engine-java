package com.github.diudiuseven.scheduledtaskenginejava.engine.core;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskExecution;

import java.time.Instant;

/**
 * 引擎事件监听：重复触发识别、跳过、状态变更等，便于观测与测试。
 */
public interface EngineListener {

    /** 同一 (taskId, fireTime) 被重复调度时回调（只产生一次有效执行）。 */
    default void onDuplicate(String taskId, Instant fireTime) {
    }

    /** 触发点被跳过（补跑窗口外 / 重叠 / 限流拒绝等）时回调。 */
    default void onSkipped(TaskExecution execution) {
    }

    /** 执行记录状态变化时回调。 */
    default void onStatusChange(TaskExecution execution) {
    }
}
