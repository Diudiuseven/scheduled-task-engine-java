package com.github.diudiuseven.scheduledtaskenginejava.engine.store;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskExecution;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存实现，主要用于测试。
 */
public class InMemoryExecutionStore implements ExecutionStore {

    private final Map<String, TaskExecution> data = new ConcurrentHashMap<>();

    private static String key(String taskId, Instant fireTime) {
        return taskId + "@" + fireTime.toEpochMilli();
    }

    @Override
    public boolean createIfAbsent(TaskExecution execution) {
        return data.putIfAbsent(key(execution.getTaskId(), execution.getFireTime()), execution) == null;
    }

    @Override
    public void update(TaskExecution execution) {
        data.put(key(execution.getTaskId(), execution.getFireTime()), execution);
    }

    @Override
    public Optional<TaskExecution> find(String taskId, Instant fireTime) {
        return Optional.ofNullable(data.get(key(taskId, fireTime)));
    }

    @Override
    public List<TaskExecution> findByTask(String taskId) {
        List<TaskExecution> result = new ArrayList<>();
        for (TaskExecution e : data.values()) {
            if (e.getTaskId().equals(taskId)) {
                result.add(e);
            }
        }
        return result;
    }

    @Override
    public List<TaskExecution> findUnfinished() {
        List<TaskExecution> result = new ArrayList<>();
        for (TaskExecution e : data.values()) {
            if (e.getStatus() == ExecutionStatus.PENDING || e.getStatus() == ExecutionStatus.RUNNING) {
                result.add(e);
            }
        }
        return result;
    }

    public List<TaskExecution> findAll() {
        return new ArrayList<>(data.values());
    }
}
