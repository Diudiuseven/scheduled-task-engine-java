package com.github.diudiuseven.scheduledtaskenginejava.engine.core;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskDefinition;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskExecution;
import com.github.diudiuseven.scheduledtaskenginejava.engine.policy.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.policy.RejectPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.ExecutionStore;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 周期任务调度引擎。
 *
 * <p>核心语义：
 * <ul>
 *   <li>每个任务按 Trigger 推进触发点，触发点以 (taskId, fireTime) 唯一标识；</li>
 *   <li>停机恢复后按 MisfirePolicy 补算错过的触发点，窗口外记录 SKIPPED；</li>
 *   <li>同一任务同一时刻最多一个有效执行，重叠按 OverlapPolicy 处理；</li>
 *   <li>失败按 RetryPolicy 退避重试，重试经由延迟调度，不阻塞其它任务；</li>
 *   <li>重复调度同一触发点只产生一次有效执行，重复触发通过 EngineListener 记录；</li>
 *   <li>触发速率与并发受限时按 RejectPolicy 排队或拒绝并记录原因；</li>
 *   <li>所有状态变更写入 ExecutionStore，重启后恢复未完成的执行。</li>
 * </ul>
 */
public class SchedulerEngine implements AutoCloseable {

    private final EngineConfig config;
    private final ExecutionStore store;
    private final Clock clock;
    private final RateLimiter rateLimiter;
    private final Semaphore globalPermits;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService workers;

    private final Map<String, TaskDefinition> tasks = new ConcurrentHashMap<>();
    private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();
    private final Map<String, Instant> cursors = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> taskPermits = new ConcurrentHashMap<>();
    /** 因限流/并发被 QUEUE 策略暂缓的执行。 */
    private final ConcurrentLinkedDeque<TaskExecution> pendingQueue = new ConcurrentLinkedDeque<>();

    private volatile EngineListener listener = new EngineListener() {
    };
    private volatile boolean running = false;

    public SchedulerEngine(EngineConfig config, ExecutionStore store, Clock clock) {
        this.config = config;
        this.store = store;
        this.clock = clock;
        this.rateLimiter = new RateLimiter(config.maxTriggersPerSecond());
        this.globalPermits = new Semaphore(config.maxGlobalConcurrency());
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "scheduler-tick");
            t.setDaemon(true);
            return t;
        });
        this.workers = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "scheduler-worker");
            t.setDaemon(true);
            return t;
        });
    }

    public void setListener(EngineListener listener) {
        this.listener = listener;
    }

    /**
     * 注册任务及其执行体。
     */
    public void register(TaskDefinition definition, TaskHandler handler) {
        tasks.put(definition.getId(), definition);
        handlers.put(definition.getId(), handler);
        taskPermits.put(definition.getId(), new Semaphore(definition.getMaxConcurrency()));
    }

    /**
     * 启动引擎：先恢复未完成任务，再补算错过的触发点，然后进入周期 tick。
     */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        recoverUnfinished();
        catchUpMissedFireTimes();
        long tickMillis = config.tickInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::tickSafely, tickMillis, tickMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 停止引擎：不再产生新触发；进行中的执行被中断并落库为 PENDING(shutdown-interrupted)，
     * 不留半完成状态，下次启动时按恢复策略继续。
     */
    @Override
    public synchronized void close() {
        if (!running) {
            return;
        }
        running = false;
        scheduler.shutdownNow();
        workers.shutdownNow();
        try {
            workers.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Instant now = clock.instant();
        for (TaskExecution e : store.findUnfinished()) {
            if (e.getStatus() == ExecutionStatus.RUNNING) {
                e.mark(ExecutionStatus.PENDING, "shutdown-interrupted", now);
                store.update(e);
            }
        }
        store.flush();
    }

    /**
     * 手动触发一次（与周期触发共用去重逻辑）。
     *
     * @return true 表示产生了新的执行记录；false 表示该触发点已存在（重复触发）。
     */
    public boolean triggerNow(String taskId) {
        TaskDefinition def = tasks.get(taskId);
        if (def == null) {
            throw new IllegalArgumentException("unknown task: " + taskId);
        }
        return onFirePoint(def, clock.instant());
    }

    /**
     * 取消一个执行：PENDING 直接置为 CANCELLED；RUNNING 置为 CANCELLED 后，
     * 执行体返回时不会再改写状态，不会留下半完成状态。
     */
    public boolean cancel(String taskId, Instant fireTime) {
        Optional<TaskExecution> found = store.find(taskId, fireTime);
        if (found.isEmpty()) {
            return false;
        }
        TaskExecution e = found.get();
        if (e.getStatus() == ExecutionStatus.PENDING || e.getStatus() == ExecutionStatus.RUNNING) {
            e.mark(ExecutionStatus.CANCELLED, "cancelled-by-user", clock.instant());
            store.update(e);
            listener.onStatusChange(e);
            return true;
        }
        return false;
    }

    public Optional<TaskExecution> findExecution(String taskId, Instant fireTime) {
        return store.find(taskId, fireTime);
    }

    public List<TaskExecution> executionsOf(String taskId) {
        return store.findByTask(taskId);
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    /** 重启恢复：在途（RUNNING）与等待中（PENDING）的执行重新进入调度，不重不漏。 */
    private void recoverUnfinished() {
        Instant now = clock.instant();
        for (TaskExecution e : store.findUnfinished()) {
            TaskDefinition def = tasks.get(e.getTaskId());
            if (def == null) {
                continue;
            }
            if (e.getStatus() == ExecutionStatus.RUNNING) {
                e.mark(ExecutionStatus.PENDING, "recovered-from-restart", now);
                store.update(e);
            }
            dispatch(def, e);
        }
    }

    /** 补算错过的触发点：从每个任务已知的最后一个触发点推进到当前时间。 */
    private void catchUpMissedFireTimes() {
        Instant now = clock.instant();
        for (TaskDefinition def : tasks.values()) {
            Instant cursor = lastKnownFireTime(def.getId()).orElse(now);
            advanceCursor(def, cursor, now);
        }
    }

    private Optional<Instant> lastKnownFireTime(String taskId) {
        Instant max = null;
        for (TaskExecution e : store.findByTask(taskId)) {
            if (max == null || e.getFireTime().isAfter(max)) {
                max = e.getFireTime();
            }
        }
        return Optional.ofNullable(max);
    }

    /** 从 cursor 推进触发点到 now 为止，返回是否全部处理完毕。 */
    private void advanceCursor(TaskDefinition def, Instant cursor, Instant now) {
        Instant current = cursor;
        while (true) {
            Optional<Instant> next = def.getTrigger().nextAfter(current);
            if (next.isEmpty() || next.get().isAfter(now)) {
                break;
            }
            onFirePoint(def, next.get());
            current = next.get();
        }
        cursors.put(def.getId(), current);
    }

    private void tickSafely() {
        try {
            Instant now = clock.instant();
            for (TaskDefinition def : tasks.values()) {
                Instant cursor = cursors.getOrDefault(def.getId(), now);
                advanceCursor(def, cursor, now);
            }
            drainPendingQueue();
        } catch (Throwable t) {
            // tick 线程不允许因单个任务异常而终止
            t.printStackTrace();
        }
    }

    /** 限流/并发暂缓的执行在每个 tick 重试进入调度。 */
    private void drainPendingQueue() {
        int budget = pendingQueue.size();
        for (int i = 0; i < budget; i++) {
            TaskExecution e = pendingQueue.poll();
            if (e == null) {
                return;
            }
            if (e.getStatus() != ExecutionStatus.PENDING) {
                continue; // 已被取消或处理
            }
            TaskDefinition def = tasks.get(e.getTaskId());
            if (def == null) {
                continue;
            }
            dispatch(def, e);
        }
    }

    /**
     * 处理一个触发点：去重 -> 补跑窗口检查 -> 调度执行。
     *
     * @return true 表示新建了执行记录。
     */
    private boolean onFirePoint(TaskDefinition def, Instant fireTime) {
        Instant now = clock.instant();
        TaskExecution execution = new TaskExecution(def.getId(), fireTime, now);
        if (!store.createIfAbsent(execution)) {
            // 重复触发：同一触发点只产生一次有效执行
            listener.onDuplicate(def.getId(), fireTime);
            return false;
        }
        if (!def.getMisfirePolicy().withinWindow(fireTime, now)) {
            execution.mark(ExecutionStatus.SKIPPED, "out-of-misfire-window", now);
            store.update(execution);
            listener.onSkipped(execution);
            return true;
        }
        dispatch(def, execution);
        return true;
    }

    /** 调度一个执行：速率限制 -> 全局并发 -> 任务内并发（重叠策略）。 */
    private void dispatch(TaskDefinition def, TaskExecution execution) {
        if (!rateLimiter.tryAcquire()) {
            onLimited(def, execution, "rate-limited");
            return;
        }
        if (!globalPermits.tryAcquire()) {
            onLimited(def, execution, "global-concurrency-limited");
            return;
        }
        Semaphore taskPermit = taskPermits.get(def.getId());
        if (def.getOverlapPolicy() == OverlapPolicy.SKIP) {
            if (!taskPermit.tryAcquire()) {
                globalPermits.release();
                execution.mark(ExecutionStatus.SKIPPED, "overlap", clock.instant());
                store.update(execution);
                listener.onSkipped(execution);
                return;
            }
            submit(def, execution, taskPermit, false);
        } else {
            // QUEUE：任一时刻仍最多 maxConcurrency 个有效执行，许可在 worker 中阻塞获取
            submit(def, execution, taskPermit, true);
        }
    }

    private void onLimited(TaskDefinition def, TaskExecution execution, String reason) {
        if (def.getRejectPolicy() == RejectPolicy.REJECT) {
            execution.mark(ExecutionStatus.SKIPPED, reason, clock.instant());
            store.update(execution);
            listener.onSkipped(execution);
        } else {
            execution.mark(ExecutionStatus.PENDING, "queued:" + reason, clock.instant());
            store.update(execution);
            pendingQueue.add(execution);
        }
    }

    private void submit(TaskDefinition def, TaskExecution execution, Semaphore taskPermit, boolean acquireInWorker) {
        workers.submit(() -> {
            boolean acquired = !acquireInWorker;
            try {
                if (acquireInWorker) {
                    taskPermit.acquire();
                    acquired = true;
                }
                runExecution(def, execution);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (execution.getStatus() != ExecutionStatus.CANCELLED) {
                    execution.mark(ExecutionStatus.PENDING, "shutdown-interrupted", clock.instant());
                    store.update(execution);
                }
            } finally {
                if (acquired) {
                    taskPermit.release();
                }
                globalPermits.release();
            }
        });
    }

    private void runExecution(TaskDefinition def, TaskExecution execution) {
        if (execution.getStatus() == ExecutionStatus.CANCELLED) {
            return;
        }
        execution.markRunning(clock.instant());
        store.update(execution);
        listener.onStatusChange(execution);
        TaskHandler handler = handlers.get(def.getId());
        try {
            handler.execute(execution);
            if (execution.getStatus() != ExecutionStatus.CANCELLED) {
                execution.mark(ExecutionStatus.SUCCESS, null, clock.instant());
                store.update(execution);
                listener.onStatusChange(execution);
            }
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                // 关闭导致的中断不算任务失败：落库为 PENDING，等待下次启动恢复
                Thread.currentThread().interrupt();
                if (execution.getStatus() != ExecutionStatus.CANCELLED) {
                    execution.mark(ExecutionStatus.PENDING, "shutdown-interrupted", clock.instant());
                    store.update(execution);
                }
                return;
            }
            onFailure(def, execution, t);
        }
    }

    private void onFailure(TaskDefinition def, TaskExecution execution, Throwable t) {
        if (execution.getStatus() == ExecutionStatus.CANCELLED) {
            return;
        }
        int attempt = execution.getAttempt();
        if (def.getRetryPolicy().hasRetryLeft(attempt)) {
            // 退避重试：通过延迟调度重新进入 dispatch，不阻塞其它任务
            execution.mark(ExecutionStatus.PENDING, "retry-waiting:" + t, clock.instant());
            store.update(execution);
            listener.onStatusChange(execution);
            long delayMillis = def.getRetryPolicy().delayAfterFailure(attempt).toMillis();
            scheduler.schedule(() -> {
                if (execution.getStatus() == ExecutionStatus.PENDING) {
                    dispatch(def, execution);
                }
            }, Math.max(delayMillis, 1), TimeUnit.MILLISECONDS);
        } else {
            execution.mark(ExecutionStatus.FAILED, String.valueOf(t), clock.instant());
            store.update(execution);
            listener.onStatusChange(execution);
        }
    }
}
