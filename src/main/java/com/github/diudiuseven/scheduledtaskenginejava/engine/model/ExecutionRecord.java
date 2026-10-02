package com.github.diudiuseven.scheduledtaskenginejava.engine.model;

import java.time.Instant;

/**
 * 一次触发点的执行记录。每个 (taskId, fireTime) 唯一对应一条记录。
 */
public final class ExecutionRecord {

    private String id;
    private String taskId;
    private Instant fireTime;
    private ExecutionStatus status;
    private int attempt;
    private int maxAttempts;
    private Instant scheduledAt;
    private Instant startedAt;
    private Instant finishedAt;
    private Instant nextRetryAt;
    private String lastError;
    private SkipReason skipReason;
    private Instant createdAt;

    public ExecutionRecord() {
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }

    public Instant getFireTime() { return fireTime; }
    public void setFireTime(Instant fireTime) { this.fireTime = fireTime; }

    public ExecutionStatus getStatus() { return status; }
    public void setStatus(ExecutionStatus status) { this.status = status; }

    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public Instant getScheduledAt() { return scheduledAt; }
    public void setScheduledAt(Instant scheduledAt) { this.scheduledAt = scheduledAt; }

    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }

    public Instant getFinishedAt() { return finishedAt; }
    public void setFinishedAt(Instant finishedAt) { this.finishedAt = finishedAt; }

    public Instant getNextRetryAt() { return nextRetryAt; }
    public void setNextRetryAt(Instant nextRetryAt) { this.nextRetryAt = nextRetryAt; }

    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }

    public SkipReason getSkipReason() { return skipReason; }
    public void setSkipReason(SkipReason skipReason) { this.skipReason = skipReason; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
