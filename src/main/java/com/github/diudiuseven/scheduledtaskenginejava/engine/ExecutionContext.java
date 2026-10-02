package com.github.diudiuseven.scheduledtaskenginejava.engine;

import java.time.Instant;

/**
 * 一次任务执行的上下文。
 */
public final class ExecutionContext {

    private final String taskId;
    private final String executionId;
    private final Instant fireTime;
    private final int attempt;

    public ExecutionContext(String taskId, String executionId, Instant fireTime, int attempt) {
        this.taskId = taskId;
        this.executionId = executionId;
        this.fireTime = fireTime;
        this.attempt = attempt;
    }

    public String getTaskId() { return taskId; }
    public String getExecutionId() { return executionId; }
    public Instant getFireTime() { return fireTime; }
    public int getAttempt() { return attempt; }

    @Override
    public String toString() {
        return "ExecutionContext{taskId='%s', fireTime=%s, attempt=%d}".formatted(taskId, fireTime, attempt);
    }
}
