package com.github.diudiuseven.scheduledtaskenginejava.engine.store;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskExecution;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 基于 JSON 文件的持久化实现：每次变更后整体原子落盘，重启时加载恢复。
 */
public class FileExecutionStore implements ExecutionStore {

    /** 可序列化的执行记录快照。 */
    public record Snapshot(String taskId, Instant fireTime, ExecutionStatus status, int attempt,
                           String reason, Instant createdAt, Instant updatedAt) {

        static Snapshot of(TaskExecution e) {
            return new Snapshot(e.getTaskId(), e.getFireTime(), e.getStatus(), e.getAttempt(),
                    e.getReason(), e.getCreatedAt(), e.getUpdatedAt());
        }

        TaskExecution toExecution() {
            TaskExecution e = new TaskExecution(taskId, fireTime, createdAt);
            e.restore(status, attempt, reason, createdAt, updatedAt);
            return e;
        }
    }

    private final InMemoryExecutionStore delegate = new InMemoryExecutionStore();
    private final Path file;
    private final ObjectMapper mapper;

    public FileExecutionStore(Path file) {
        this.file = file;
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        load();
    }

    private void load() {
        if (!Files.exists(file)) {
            return;
        }
        try {
            JavaType type = mapper.getTypeFactory().constructCollectionType(List.class, Snapshot.class);
            List<Snapshot> snapshots = mapper.readValue(file.toFile(), type);
            for (Snapshot s : snapshots) {
                delegate.createIfAbsent(s.toExecution());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load execution store from " + file, e);
        }
    }

    @Override
    public synchronized boolean createIfAbsent(TaskExecution execution) {
        boolean created = delegate.createIfAbsent(execution);
        if (created) {
            persist();
        }
        return created;
    }

    @Override
    public synchronized void update(TaskExecution execution) {
        delegate.update(execution);
        persist();
    }

    @Override
    public Optional<TaskExecution> find(String taskId, Instant fireTime) {
        return delegate.find(taskId, fireTime);
    }

    @Override
    public List<TaskExecution> findByTask(String taskId) {
        return delegate.findByTask(taskId);
    }

    @Override
    public List<TaskExecution> findUnfinished() {
        return delegate.findUnfinished();
    }

    @Override
    public synchronized void flush() {
        persist();
    }

    private void persist() {
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            List<Snapshot> snapshots = new ArrayList<>();
            for (TaskExecution e : delegate.findAll()) {
                snapshots.add(Snapshot.of(e));
            }
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), snapshots);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist execution store to " + file, e);
        }
    }
}
