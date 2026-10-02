package com.github.diudiuseven.scheduledtaskenginejava.engine.store;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskExecution;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 触发与执行状态的持久化存储。
 * 重启后据此区分已完成、在途与失败的执行并恢复。
 */
public interface ExecutionStore {

    /**
     * 若 (taskId, fireTime) 不存在则创建并返回 true；已存在则返回 false（重复触发识别）。
     */
    boolean createIfAbsent(TaskExecution execution);

    /**
     * 更新已有执行记录。
     */
    void update(TaskExecution execution);

    Optional<TaskExecution> find(String taskId, Instant fireTime);

    List<TaskExecution> findByTask(String taskId);

    /**
     * 找出所有未完成（PENDING / RUNNING）的记录，用于重启恢复。
     */
    List<TaskExecution> findUnfinished();

    /**
     * 将内存中的状态刷入持久层（内存实现可为 no-op）。
     */
    default void flush() {
    }
}
