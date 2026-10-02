package com.github.diudiuseven.scheduledtaskenginejava.engine.store;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionRecord;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.SkipReason;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 触发与执行状态持久化。每个 (taskId, fireTime) 唯一对应一条执行记录。
 */
public interface ExecutionStore {

    /**
     * 插入一条待调度执行记录。
     *
     * @return 新记录；若 (taskId, fireTime) 已存在则返回空（去重）
     */
    Optional<ExecutionRecord> insertPending(String taskId, Instant fireTime, Instant scheduledAt, int maxAttempts);

    /** 为没有待执行记录的触发点插入一条跳过记录（补跑窗口外、重叠跳过等）。 */
    ExecutionRecord recordSkip(String taskId, Instant fireTime, SkipReason reason, Instant at);

    /** 记录一次重复触发事件。 */
    void recordDuplicate(String taskId, Instant fireTime, SkipReason reason, Instant at);

    /** 该任务是否存在未完成（PENDING/QUEUED/RUNNING/RETRY_WAIT）的执行。 */
    boolean existsUnfinished(String taskId);

    /** 该任务已持久化的最近触发点（无记录返回空）。 */
    Optional<Instant> lastFireTime(String taskId);

    /** 恢复用：所有未到终态的记录（PENDING/QUEUED/RUNNING/RETRY_WAIT）。 */
    List<ExecutionRecord> findRecoverable();

    Optional<ExecutionRecord> findById(String id);

    List<ExecutionRecord> findByTask(String taskId);

    void markRunning(String id, Instant startedAt);

    void markQueued(String id, Instant at);

    void markSucceeded(String id, Instant finishedAt);

    void markRetryWait(String id, Instant nextRetryAt, String error);

    void markFailed(String id, Instant finishedAt, String error);

    void markSkipped(String id, Instant at, SkipReason reason);

    void markCancelled(String id, Instant at);

    long countByStatus(ExecutionStatus status);

    /** 某任务的重复触发事件数。 */
    long countDuplicateEvents(String taskId);
}
