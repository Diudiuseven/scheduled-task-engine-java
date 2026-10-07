package com.github.diudiuseven.scheduledtaskenginejava.engine.store;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionRecord;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.SkipReason;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 基于 JDBC（H2 文件库）的持久化实现。时间以 UTC 存储。
 */
public class JdbcExecutionStore implements ExecutionStore {

    private static final String SCHEMA = """
            CREATE TABLE IF NOT EXISTS execution_record (
              id VARCHAR(64) PRIMARY KEY,
              task_id VARCHAR(128) NOT NULL,
              fire_time TIMESTAMP(9) NOT NULL,
              status VARCHAR(32) NOT NULL,
              attempt INT NOT NULL,
              max_attempts INT NOT NULL,
              scheduled_at TIMESTAMP(9),
              started_at TIMESTAMP(9),
              finished_at TIMESTAMP(9),
              next_retry_at TIMESTAMP(9),
              last_error VARCHAR(2000),
              skip_reason VARCHAR(64),
              created_at TIMESTAMP(9) NOT NULL,
              CONSTRAINT uq_fire UNIQUE (task_id, fire_time)
            );
            CREATE INDEX IF NOT EXISTS idx_task_fire ON execution_record(task_id, fire_time);
            CREATE INDEX IF NOT EXISTS idx_status ON execution_record(status);

            CREATE TABLE IF NOT EXISTS duplicate_log (
              id VARCHAR(64) PRIMARY KEY,
              task_id VARCHAR(128) NOT NULL,
              fire_time TIMESTAMP(9) NOT NULL,
              reason VARCHAR(64) NOT NULL,
              detected_at TIMESTAMP(9) NOT NULL
            );
            """;

    private static final RowMapper<ExecutionRecord> ROW_MAPPER = new RowMapper<>() {
        @Override
        public ExecutionRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            ExecutionRecord r = new ExecutionRecord();
            r.setId(rs.getString("id"));
            r.setTaskId(rs.getString("task_id"));
            r.setFireTime(readInstant(rs, "fire_time"));
            r.setStatus(ExecutionStatus.valueOf(rs.getString("status")));
            r.setAttempt(rs.getInt("attempt"));
            r.setMaxAttempts(rs.getInt("max_attempts"));
            r.setScheduledAt(readInstant(rs, "scheduled_at"));
            r.setStartedAt(readInstant(rs, "started_at"));
            r.setFinishedAt(readInstant(rs, "finished_at"));
            r.setNextRetryAt(readInstant(rs, "next_retry_at"));
            r.setLastError(rs.getString("last_error"));
            String skip = rs.getString("skip_reason");
            r.setSkipReason(skip == null ? null : SkipReason.valueOf(skip));
            r.setCreatedAt(readInstant(rs, "created_at"));
            return r;
        }
    };

    private final JdbcTemplate jdbc;

    public JdbcExecutionStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        this.jdbc.execute(SCHEMA);
    }

    private static LocalDateTime writeInstant(Instant instant) {
        return instant == null ? null : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Instant readInstant(ResultSet rs, String column) throws SQLException {
        LocalDateTime ldt = rs.getObject(column, LocalDateTime.class);
        return ldt == null ? null : ldt.toInstant(ZoneOffset.UTC);
    }

    @Override
    public Optional<ExecutionRecord> insertPending(String taskId, Instant fireTime, Instant scheduledAt, int maxAttempts) {
        String id = UUID.randomUUID().toString();
        // createdAt 与 scheduledAt 同源（引擎时钟），保证等待超时等判断时钟基准一致
        Instant now = scheduledAt;
        try {
            jdbc.update("""
                    INSERT INTO execution_record
                      (id, task_id, fire_time, status, attempt, max_attempts, scheduled_at, created_at)
                    VALUES (?, ?, ?, ?, 0, ?, ?, ?)
                    """,
                    id, taskId, writeInstant(fireTime), ExecutionStatus.PENDING.name(),
                    maxAttempts, writeInstant(scheduledAt), writeInstant(now));
        } catch (DuplicateKeyException e) {
            return Optional.empty();
        }
        ExecutionRecord r = new ExecutionRecord();
        r.setId(id);
        r.setTaskId(taskId);
        r.setFireTime(fireTime);
        r.setStatus(ExecutionStatus.PENDING);
        r.setAttempt(0);
        r.setMaxAttempts(maxAttempts);
        r.setScheduledAt(scheduledAt);
        r.setCreatedAt(now);
        return Optional.of(r);
    }

    @Override
    public ExecutionRecord recordSkip(String taskId, Instant fireTime, SkipReason reason, Instant at) {
        String id = UUID.randomUUID().toString();
        try {
            jdbc.update("""
                    INSERT INTO execution_record
                      (id, task_id, fire_time, status, attempt, max_attempts, finished_at, skip_reason, created_at)
                    VALUES (?, ?, ?, 'SKIPPED', 0, 0, ?, ?, ?)
                    """,
                    id, taskId, writeInstant(fireTime), writeInstant(at), reason.name(), writeInstant(at));
        } catch (DuplicateKeyException e) {
            return findByTaskAndFireTime(taskId, fireTime).orElse(null);
        }
        ExecutionRecord r = new ExecutionRecord();
        r.setId(id);
        r.setTaskId(taskId);
        r.setFireTime(fireTime);
        r.setStatus(ExecutionStatus.SKIPPED);
        r.setSkipReason(reason);
        r.setFinishedAt(at);
        r.setCreatedAt(at);
        return r;
    }

    @Override
    public void recordDuplicate(String taskId, Instant fireTime, SkipReason reason, Instant at) {
        jdbc.update("""
                INSERT INTO duplicate_log (id, task_id, fire_time, reason, detected_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                UUID.randomUUID().toString(), taskId, writeInstant(fireTime), reason.name(), writeInstant(at));
    }

    @Override
    public boolean existsUnfinished(String taskId) {
        Long n = jdbc.queryForObject("""
                SELECT COUNT(*) FROM execution_record
                WHERE task_id = ? AND status IN ('PENDING', 'QUEUED', 'RUNNING', 'RETRY_WAIT')
                """, Long.class, taskId);
        return n != null && n > 0;
    }

    @Override
    public Optional<Instant> lastFireTime(String taskId) {
        List<Instant> times = jdbc.query("""
                SELECT MAX(fire_time) FROM execution_record WHERE task_id = ?
                """, (rs, rowNum) -> {
                    LocalDateTime ldt = rs.getObject(1, LocalDateTime.class);
                    return ldt == null ? null : ldt.toInstant(ZoneOffset.UTC);
                }, taskId);
        return times.isEmpty() || times.get(0) == null ? Optional.empty() : Optional.of(times.get(0));
    }

    @Override
    public List<ExecutionRecord> findRecoverable() {
        return jdbc.query("""
                SELECT * FROM execution_record
                WHERE status IN ('PENDING', 'QUEUED', 'RUNNING', 'RETRY_WAIT')
                ORDER BY fire_time ASC
                """, ROW_MAPPER);
    }

    @Override
    public Optional<ExecutionRecord> findById(String id) {
        List<ExecutionRecord> list = jdbc.query(
                "SELECT * FROM execution_record WHERE id = ?", ROW_MAPPER, id);
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    @Override
    public List<ExecutionRecord> findByTask(String taskId) {
        return jdbc.query(
                "SELECT * FROM execution_record WHERE task_id = ? ORDER BY fire_time ASC", ROW_MAPPER, taskId);
    }

    public Optional<ExecutionRecord> findByTaskAndFireTime(String taskId, Instant fireTime) {
        List<ExecutionRecord> list = jdbc.query(
                "SELECT * FROM execution_record WHERE task_id = ? AND fire_time = ?",
                ROW_MAPPER, taskId, writeInstant(fireTime));
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    /**
     * 状态单调推进：所有状态变更都带前置状态守卫，已进入终态
     * （SUCCEEDED/FAILED/SKIPPED/CANCELLED）的记录不会被任何并发记账动作改回。
     */
    private static final String NON_TERMINAL_GUARD =
            "status IN ('PENDING', 'QUEUED', 'RUNNING', 'RETRY_WAIT')";

    @Override
    public boolean markRunning(String id, Instant startedAt) {
        return jdbc.update("""
                UPDATE execution_record SET status = 'RUNNING', started_at = ?
                WHERE id = ? AND status IN ('PENDING', 'QUEUED', 'RETRY_WAIT')
                """, writeInstant(startedAt), id) > 0;
    }

    @Override
    public boolean markQueued(String id, Instant at) {
        return jdbc.update("""
                UPDATE execution_record SET status = 'QUEUED', scheduled_at = ?
                WHERE id = ? AND status = 'PENDING'
                """, writeInstant(at), id) > 0;
    }

    @Override
    public boolean markSucceeded(String id, Instant finishedAt) {
        return jdbc.update("""
                UPDATE execution_record SET status = 'SUCCEEDED', finished_at = ?
                WHERE id = ? AND status = 'RUNNING'
                """, writeInstant(finishedAt), id) > 0;
    }

    @Override
    public boolean markRetryWait(String id, Instant nextRetryAt, String error) {
        return jdbc.update("""
                UPDATE execution_record
                SET status = 'RETRY_WAIT', attempt = attempt + 1, next_retry_at = ?, last_error = ?
                WHERE id = ? AND status = 'RUNNING'
                """, writeInstant(nextRetryAt), truncate(error), id) > 0;
    }

    @Override
    public boolean markFailed(String id, Instant finishedAt, String error) {
        return jdbc.update("""
                UPDATE execution_record
                SET status = 'FAILED', attempt = attempt + 1, finished_at = ?, last_error = ?
                WHERE id = ? AND\s""" + NON_TERMINAL_GUARD,
                writeInstant(finishedAt), truncate(error), id) > 0;
    }

    @Override
    public boolean markSkipped(String id, Instant at, SkipReason reason) {
        return jdbc.update("""
                UPDATE execution_record SET status = 'SKIPPED', finished_at = ?, skip_reason = ?
                WHERE id = ? AND\s""" + NON_TERMINAL_GUARD,
                writeInstant(at), reason.name(), id) > 0;
    }

    @Override
    public boolean markCancelled(String id, Instant at) {
        return jdbc.update("""
                UPDATE execution_record SET status = 'CANCELLED', finished_at = ?
                WHERE id = ? AND\s""" + NON_TERMINAL_GUARD,
                writeInstant(at), id) > 0;
    }

    @Override
    public long countByStatus(ExecutionStatus status) {
        Long n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM execution_record WHERE status = ?", Long.class, status.name());
        return n == null ? 0 : n;
    }

    @Override
    public long countDuplicateEvents(String taskId) {
        Long n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM duplicate_log WHERE task_id = ?", Long.class, taskId);
        return n == null ? 0 : n;
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() > 1900 ? s.substring(0, 1900) : s;
    }
}
