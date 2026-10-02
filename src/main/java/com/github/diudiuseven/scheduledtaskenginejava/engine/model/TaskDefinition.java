package com.github.diudiuseven.scheduledtaskenginejava.engine.model;

import com.github.diudiuseven.scheduledtaskenginejava.engine.policy.MisfirePolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.policy.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.policy.RejectPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.policy.RetryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.Trigger;

import java.util.Objects;

/**
 * 任务定义：触发规则 + 各类既定策略。
 */
public class TaskDefinition {

    private final String id;
    private final Trigger trigger;
    private final RetryPolicy retryPolicy;
    private final MisfirePolicy misfirePolicy;
    private final OverlapPolicy overlapPolicy;
    private final RejectPolicy rejectPolicy;
    /** 单任务并发度上限，>= 1；同一任务最多一个有效执行时应为 1。 */
    private final int maxConcurrency;

    private TaskDefinition(Builder b) {
        this.id = Objects.requireNonNull(b.id, "id");
        this.trigger = Objects.requireNonNull(b.trigger, "trigger");
        this.retryPolicy = b.retryPolicy;
        this.misfirePolicy = b.misfirePolicy;
        this.overlapPolicy = b.overlapPolicy;
        this.rejectPolicy = b.rejectPolicy;
        this.maxConcurrency = b.maxConcurrency;
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be >= 1");
        }
    }

    public String getId() {
        return id;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public MisfirePolicy getMisfirePolicy() {
        return misfirePolicy;
    }

    public OverlapPolicy getOverlapPolicy() {
        return overlapPolicy;
    }

    public RejectPolicy getRejectPolicy() {
        return rejectPolicy;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public static Builder builder(String id, Trigger trigger) {
        return new Builder(id, trigger);
    }

    public static class Builder {
        private final String id;
        private final Trigger trigger;
        private RetryPolicy retryPolicy = RetryPolicy.NO_RETRY;
        private MisfirePolicy misfirePolicy = MisfirePolicy.DEFAULT;
        private OverlapPolicy overlapPolicy = OverlapPolicy.SKIP;
        private RejectPolicy rejectPolicy = RejectPolicy.QUEUE;
        private int maxConcurrency = 1;

        private Builder(String id, Trigger trigger) {
            this.id = id;
            this.trigger = trigger;
        }

        public Builder retryPolicy(RetryPolicy p) {
            this.retryPolicy = p;
            return this;
        }

        public Builder misfirePolicy(MisfirePolicy p) {
            this.misfirePolicy = p;
            return this;
        }

        public Builder overlapPolicy(OverlapPolicy p) {
            this.overlapPolicy = p;
            return this;
        }

        public Builder rejectPolicy(RejectPolicy p) {
            this.rejectPolicy = p;
            return this;
        }

        public Builder maxConcurrency(int n) {
            this.maxConcurrency = n;
            return this;
        }

        public TaskDefinition build() {
            return new TaskDefinition(this);
        }
    }
}
