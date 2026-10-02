package com.github.diudiuseven.scheduledtaskenginejava.engine.model;

import java.time.Instant;
import java.util.Objects;

/**
 * 一次触发点对应的执行记录。以 (taskId, fireTime) 作为唯一标识，
 * 同一触发点因重试或补跑被重复调度时只会产生一条记录。
 */
public class TaskExecution {

    private final String taskId;
    private final Instant fireTime;
    private volatile ExecutionStatus status;
    private volatile int attempt;
    private volatile String reason;
    private volatile Instant createdAt;
    private volatile Instant updatedAt;

    public TaskExecution(String taskId, Instant fireTime, Instant now) {
        this.taskId = Objects.requireNonNull(taskId, "taskId");
        this.fireTime = Objects.requireNonNull(fireTime, "fireTime");
        this.status = ExecutionStatus.PENDING;
        this.attempt = 0;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public String getTaskId() {
        return taskId;
    }

    public Instant getFireTime() {
        return fireTime;
    }

    public ExecutionStatus getStatus() {
        return status;
    }

    public int getAttempt() {
        return attempt;
    }

    public String getReason() {
        return reason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void markRunning(Instant now) {
        this.status = ExecutionStatus.RUNNING;
        this.attempt++;
        this.updatedAt = now;
    }

    public void mark(ExecutionStatus status, String reason, Instant now) {
        this.status = status;
        this.reason = reason;
        this.updatedAt = now;
    }

    /** 恢复用 setter，仅用于持久化层重建对象。 */
    public void restore(ExecutionStatus status, int attempt, String reason, Instant createdAt, Instant updatedAt) {
        this.status = status;
        this.attempt = attempt;
        this.reason = reason;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }
}
