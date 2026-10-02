package com.github.diudiuseven.scheduledtaskenginejava.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.CatchUpPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RecoveryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RetryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.TriggerDefinition;

import java.time.Instant;
import java.util.Objects;

/**
 * 任务定义：标识、触发定义、处理器与各类策略。
 */
public final class TaskDefinition {

    private final String id;
    private final String name;
    private final TriggerDefinition trigger;
    private final TaskHandler handler;
    private final OverlapPolicy overlapPolicy;
    private final CatchUpPolicy catchUpPolicy;
    private final RetryPolicy retryPolicy;
    private final RecoveryPolicy recoveryPolicy;
    private final int maxConcurrency;
    private final Instant startTime;

    private TaskDefinition(Builder b) {
        this.id = Objects.requireNonNull(b.id, "id");
        this.name = b.name == null ? b.id : b.name;
        this.trigger = Objects.requireNonNull(b.trigger, "trigger");
        this.handler = Objects.requireNonNull(b.handler, "handler");
        this.overlapPolicy = b.overlapPolicy == null ? OverlapPolicy.SKIP : b.overlapPolicy;
        this.catchUpPolicy = b.catchUpPolicy == null ? CatchUpPolicy.disabled() : b.catchUpPolicy;
        this.retryPolicy = b.retryPolicy == null ? RetryPolicy.noRetry() : b.retryPolicy;
        this.recoveryPolicy = b.recoveryPolicy == null ? RecoveryPolicy.RETRY : b.recoveryPolicy;
        this.maxConcurrency = b.maxConcurrency <= 0 ? 1 : b.maxConcurrency;
        this.startTime = b.startTime == null ? Instant.now() : b.startTime;
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public TriggerDefinition getTrigger() { return trigger; }
    public TaskHandler getHandler() { return handler; }
    public OverlapPolicy getOverlapPolicy() { return overlapPolicy; }
    public CatchUpPolicy getCatchUpPolicy() { return catchUpPolicy; }
    public RetryPolicy getRetryPolicy() { return retryPolicy; }
    public RecoveryPolicy getRecoveryPolicy() { return recoveryPolicy; }
    public int getMaxConcurrency() { return maxConcurrency; }
    public Instant getStartTime() { return startTime; }

    public static Builder builder(String id, TriggerDefinition trigger, TaskHandler handler) {
        return new Builder(id, trigger, handler);
    }

    public static final class Builder {
        private final String id;
        private String name;
        private final TriggerDefinition trigger;
        private final TaskHandler handler;
        private OverlapPolicy overlapPolicy;
        private CatchUpPolicy catchUpPolicy;
        private RetryPolicy retryPolicy;
        private RecoveryPolicy recoveryPolicy;
        private int maxConcurrency = 1;
        private Instant startTime;

        private Builder(String id, TriggerDefinition trigger, TaskHandler handler) {
            this.id = id;
            this.trigger = trigger;
            this.handler = handler;
        }

        public Builder name(String name) { this.name = name; return this; }
        public Builder overlapPolicy(OverlapPolicy p) { this.overlapPolicy = p; return this; }
        public Builder catchUpPolicy(CatchUpPolicy p) { this.catchUpPolicy = p; return this; }
        public Builder retryPolicy(RetryPolicy p) { this.retryPolicy = p; return this; }
        public Builder recoveryPolicy(RecoveryPolicy p) { this.recoveryPolicy = p; return this; }
        public Builder maxConcurrency(int c) { this.maxConcurrency = c; return this; }
        public Builder startTime(Instant t) { this.startTime = t; return this; }

        public TaskDefinition build() { return new TaskDefinition(this); }
    }
}
