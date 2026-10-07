package com.github.diudiuseven.scheduledtaskenginejava.engine;

import com.github.diudiuseven.scheduledtaskenginejava.engine.config.EngineProperties;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.CatchUpPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionRecord;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.ExecutionStatus;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.OverlapPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RecoveryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.RetryPolicy;
import com.github.diudiuseven.scheduledtaskenginejava.engine.model.SkipReason;
import com.github.diudiuseven.scheduledtaskenginejava.engine.store.ExecutionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 周期任务触发与执行引擎。
 *
 * 语义概览（详见 README）：
 * - 触发点由 {@link com.github.diudiuseven.scheduledtaskenginejava.engine.trigger.TriggerDefinition}
 *   按带时区的固定间隔/cron 计算，DST 切换语义确定可复现；
 * - 每个触发点唯一对应一条执行记录 (taskId, fireTime)，重复调度只产生一次有效执行，重复事件落库；
 * - 执行失败按退避策略重试，重试进入延迟队列、不阻塞其它任务；
 * - 重启后持久化状态可区分已完成/在途/失败，并按策略恢复（在途视为一次失败尝试）；
 * - 触发速率与单任务并发受限，超限排队或拒绝并记录原因；
 * - 取消通过中断工作线程实现，状态一致落库，不留下半完成状态。
 */
public class ScheduledTaskEngine {

    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskEngine.class);

    /** 排队等待（信号量/限流）时的重入队延迟。 */
    private static final long REQUEUE_DELAY_MS = 100L;

    private final Clock clock;
    private final ExecutionStore store;
    private final EngineProperties properties;
    private final Map<String, TaskDefinition> tasks = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> semaphores = new ConcurrentHashMap<>();
    private final Map<String, Thread> runningThreads = new ConcurrentHashMap<>();
    private final DelayQueue<ExecutionCmd> queue = new DelayQueue<>();
    private final RateLimiter rateLimiter;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private ExecutorService workerPool;
    private ScheduledExecutorService triggerScheduler;

    public ScheduledTaskEngine(Clock clock, ExecutionStore store, EngineProperties properties) {
        this.clock = clock;
        this.store = store;
        this.properties = properties;
        this.rateLimiter = new RateLimiter(properties.getRateLimit());
    }

    /** 注册任务。引擎启动后注册的任务会在下一个调度周期生效。 */
    public void register(TaskDefinition task) {
        tasks.put(task.getId(), task);
        semaphores.put(task.getId(), new Semaphore(task.getMaxConcurrency()));
        log.info("Registered task '{}' trigger={} overlap={} catchUp={} retry={}",
                task.getId(), task.getTrigger(), task.getOverlapPolicy(),
                task.getCatchUpPolicy(), task.getRetryPolicy());
    }

    /** 启动引擎：恢复在途状态，启动工作线程与触发调度。 */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        for (ExecutionRecord rec : store.findRecoverable()) {
            recover(rec);
        }
        workerPool = Executors.newFixedThreadPool(properties.getWorkerPoolSize(), r -> {
            Thread t = new Thread(r, "engine-worker");
            t.setDaemon(true);
            return t;
        });
        for (int i = 0; i < properties.getWorkerPoolSize(); i++) {
            workerPool.submit(this::workerLoop);
        }
        triggerScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "engine-trigger");
            t.setDaemon(true);
            return t;
        });
        triggerScheduler.scheduleAtFixedRate(this::safeTick, 0,
                properties.getTickInterval().toMillis(), TimeUnit.MILLISECONDS);
        log.info("Scheduled task engine started (workers={}, rateLimit={}/s, queue={})",
                properties.getWorkerPoolSize(), properties.getRateLimit(), properties.getQueueCapacity());
    }

    /** 停止引擎：中断工作线程并等待其结束。在途执行保留 RUNNING 状态，下次启动按策略恢复。 */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        triggerScheduler.shutdownNow();
        workerPool.shutdownNow();
        queue.clear();
        try {
            if (!workerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Engine workers did not terminate within 5s; in-flight executions remain RUNNING for recovery");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Scheduled task engine stopped");
    }

    public boolean isRunning() {
        return running.get();
    }

    /** 手动调度某任务的一个触发点（运维补偿/测试）。重复触发返回 false 并落库。 */
    public boolean scheduleFirePoint(String taskId, Instant fireTime) {
        TaskDefinition task = tasks.get(taskId);
        if (task == null) {
            throw new IllegalArgumentException("Unknown task: " + taskId);
        }
        return enqueueExecution(task, fireTime, clock.instant());
    }

    /** 取消一次执行：排队中则移除，执行中则中断线程。 */
    public boolean cancel(String executionId) {
        Optional<ExecutionRecord> opt = store.findById(executionId);
        if (opt.isEmpty()) {
            return false;
        }
        ExecutionRecord rec = opt.get();
        if (rec.getStatus().isTerminal()) {
            return false;
        }
        Instant now = clock.instant();
        boolean removed = queue.removeIf(cmd -> cmd.executionId.equals(executionId));
        if (removed) {
            store.markCancelled(executionId, now);
            log.info("Cancelled queued execution {}", executionId);
            return true;
        }
        Thread worker = runningThreads.get(executionId);
        if (worker != null && store.markCancelled(executionId, now)) {
            // 条件取消成功才中断：若记录已并发到达终态，不打扰工作线程
            worker.interrupt();
            log.info("Cancelled running execution {}", executionId);
            return true;
        }
        return false;
    }

    public List<ExecutionRecord> executionsOf(String taskId) {
        return store.findByTask(taskId);
    }

    public Clock clock() {
        return clock;
    }

    public ExecutionStore store() {
        return store;
    }

    // ------------------------------------------------------------------
    // 触发调度
    // ------------------------------------------------------------------

    private void safeTick() {
        try {
            Instant now = clock.instant();
            for (TaskDefinition task : tasks.values()) {
                try {
                    tickTask(task, now);
                } catch (Exception e) {
                    log.error("Trigger tick failed for task {}", task.getId(), e);
                }
            }
        } catch (Exception e) {
            log.error("Trigger tick error", e);
        }
    }

    private void tickTask(TaskDefinition task, Instant now) {
        Instant from = store.lastFireTime(task.getId()).orElse(task.getStartTime());
        List<Instant> points = task.getTrigger().firePointsBetween(from, now);
        for (Instant fire : points) {
            handleFirePoint(task, fire, now);
        }
    }

    private void handleFirePoint(TaskDefinition task, Instant fire, Instant now) {
        CatchUpPolicy cup = task.getCatchUpPolicy();
        if (cup.enabled() && fire.isBefore(now.minus(cup.window()))) {
            ExecutionRecord skipped = store.recordSkip(task.getId(), fire, SkipReason.CATCHUP_WINDOW, now);
            log.info("Skip fire point {} of task {}: beyond catch-up window {}", fire, task.getId(), cup.window());
            return;
        }
        if (task.getOverlapPolicy() == OverlapPolicy.SKIP && store.existsUnfinished(task.getId())) {
            store.recordSkip(task.getId(), fire, SkipReason.OVERLAP, now);
            log.info("Skip fire point {} of task {}: previous execution unfinished (overlap=SKIP)", fire, task.getId());
            return;
        }
        enqueueExecution(task, fire, now);
    }

    /** @return true=新入队；false=重复触发已落库 */
    private boolean enqueueExecution(TaskDefinition task, Instant fire, Instant scheduledAt) {
        Optional<ExecutionRecord> rec = store.insertPending(
                task.getId(), fire, scheduledAt, task.getRetryPolicy().maxAttempts());
        if (rec.isEmpty()) {
            store.recordDuplicate(task.getId(), fire, SkipReason.DUPLICATE, scheduledAt);
            log.info("Duplicate fire point {} for task {} detected and logged", fire, task.getId());
            return false;
        }
        ExecutionCmd cmd = newCmd(rec.get().getId(), task.getId(), fire, scheduledAt);
        // 容量检查与入队在同一临界区内，避免并发登记同时越过容量上限
        synchronized (queue) {
            if (queue.size() >= properties.getQueueCapacity()) {
                store.markSkipped(rec.get().getId(), clock.instant(), SkipReason.QUEUE_FULL);
                log.warn("Queue full (capacity={}), skipped execution {} of task {}",
                        properties.getQueueCapacity(), rec.get().getId(), task.getId());
                return true;
            }
            queue.offer(cmd);
        }
        // 条件推进 PENDING -> QUEUED：若 worker 已抢先执行完毕或记录被并发取消，
        // 此处不再生效，状态保持单调不回退
        store.markQueued(rec.get().getId(), scheduledAt);
        return true;
    }

    // ------------------------------------------------------------------
    // 执行与重试
    // ------------------------------------------------------------------

    private void workerLoop() {
        while (running.get()) {
            try {
                ExecutionCmd cmd = queue.poll(200, TimeUnit.MILLISECONDS);
                if (cmd != null) {
                    runOne(cmd);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("Worker loop error", e);
            }
        }
    }

    private void runOne(ExecutionCmd cmd) {
        TaskDefinition task = tasks.get(cmd.taskId);
        if (task == null) {
            log.warn("Task {} disappeared, dropping execution {}", cmd.taskId, cmd.executionId);
            return;
        }
        ExecutionRecord rec = store.findById(cmd.executionId).orElse(null);
        if (rec == null || rec.getStatus().isTerminal()) {
            // 已终态（成功/失败/跳过/取消）的记录不再执行，重复队列元素直接丢弃
            return;
        }
        Semaphore semaphore = semaphores.computeIfAbsent(task.getId(), k -> new Semaphore(task.getMaxConcurrency()));

        // 单任务并发度：获取不到则排队等待（重入延迟队列），超时则拒绝
        boolean acquired;
        try {
            acquired = semaphore.tryAcquire(REQUEUE_DELAY_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        if (!acquired) {
            if (isWaitTimedOut(rec)) {
                store.markSkipped(cmd.executionId, clock.instant(), SkipReason.CONCURRENCY_LIMITED);
                log.warn("Concurrency limit wait timed out for task {}, skipped execution {}",
                        task.getId(), cmd.executionId);
            } else {
                requeue(cmd);
            }
            return;
        }
        try {
            // 触发速率：获取不到则排队等待，超时则拒绝
            boolean rateOk;
            try {
                rateOk = rateLimiter.tryAcquire(REQUEUE_DELAY_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (!rateOk) {
                if (isWaitTimedOut(rec)) {
                    store.markSkipped(cmd.executionId, clock.instant(), SkipReason.RATE_LIMITED);
                    log.warn("Rate limit wait timed out, skipped execution {}", cmd.executionId);
                } else {
                    requeue(cmd);
                }
                return;
            }
            // 进入 RUNNING 是执行的准入闸门：条件更新失败说明记录已被并发地
            // 取消/跳过/完成，直接放弃，保证同一执行记录最多一次有效执行。
            if (!store.markRunning(cmd.executionId, clock.instant())) {
                log.info("Execution {} no longer runnable (concurrent state change), dropped",
                        cmd.executionId);
                return;
            }
            runningThreads.put(cmd.executionId, Thread.currentThread());
            try {
                task.getHandler().execute(
                        new ExecutionContext(task.getId(), cmd.executionId, cmd.fireTime, rec.getAttempt()));
                if (Thread.currentThread().isInterrupted()) {
                    // 运行期间被中断（取消或引擎关闭）：状态由取消方/恢复流程决定
                    log.info("Execution {} interrupted during run, state left to canceller/recovery",
                            cmd.executionId);
                } else if (!store.markSucceeded(cmd.executionId, clock.instant())) {
                    // 并发取消先落库：CANCELLED 不被覆盖
                    log.info("Execution {} completed but was concurrently cancelled", cmd.executionId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (store.findById(cmd.executionId).map(r -> r.getStatus() == ExecutionStatus.CANCELLED).orElse(false)) {
                    log.info("Execution {} cancelled by user", cmd.executionId);
                } else {
                    // 引擎关闭导致的中断：保留 RUNNING，下次启动恢复
                    log.info("Execution {} interrupted by shutdown, will recover on restart", cmd.executionId);
                }
            } catch (Exception e) {
                handleFailure(task, cmd, rec, e);
            } finally {
                runningThreads.remove(cmd.executionId);
            }
        } finally {
            semaphore.release();
        }
    }

    private boolean isWaitTimedOut(ExecutionRecord rec) {
        return clock.instant().isAfter(rec.getCreatedAt().plus(properties.getAcquireTimeout()));
    }

    /** 构造队列元素：按引擎时钟计算剩余延迟，并换算为 wall-clock 到期时刻。 */
    private ExecutionCmd newCmd(String executionId, String taskId, Instant fireTime, Instant scheduledAt) {
        long delayMs = Math.max(0, scheduledAt.toEpochMilli() - clock.instant().toEpochMilli());
        return new ExecutionCmd(executionId, taskId, fireTime, scheduledAt,
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMs));
    }

    private void requeue(ExecutionCmd cmd) {
        Instant next = clock.instant().plusMillis(REQUEUE_DELAY_MS);
        queue.offer(newCmd(cmd.executionId, cmd.taskId, cmd.fireTime, next));
    }

    private void handleFailure(TaskDefinition task, ExecutionCmd cmd, ExecutionRecord rec, Exception e) {
        RetryPolicy policy = task.getRetryPolicy();
        int attempt = rec.getAttempt() + 1;
        if (attempt >= policy.maxAttempts()) {
            store.markFailed(cmd.executionId, clock.instant(), abbreviate(e));
            log.warn("Execution {} of task {} failed after {} attempts",
                    cmd.executionId, task.getId(), attempt, e);
            return;
        }
        Instant retryAt = clock.instant().plus(policy.backoffAfter(attempt));
        if (store.markRetryWait(cmd.executionId, retryAt, abbreviate(e))) {
            queue.offer(newCmd(cmd.executionId, task.getId(), cmd.fireTime, retryAt));
            log.info("Execution {} of task {} failed (attempt {}), retry at {}",
                    cmd.executionId, task.getId(), attempt, retryAt, e);
        } else {
            // 并发取消/关闭已推进状态：不再安排重试，避免复活终态记录
            log.info("Execution {} of task {} failed but state already changed; no retry enqueued",
                    cmd.executionId, task.getId());
        }
    }

    // ------------------------------------------------------------------
    // 重启恢复
    // ------------------------------------------------------------------

    private void recover(ExecutionRecord rec) {
        TaskDefinition task = tasks.get(rec.getTaskId());
        if (task == null) {
            log.warn("Recovering execution {} but task {} not registered; marking FAILED",
                    rec.getId(), rec.getTaskId());
            store.markFailed(rec.getId(), clock.instant(), "task not registered on recovery");
            return;
        }
        switch (rec.getStatus()) {
            case RUNNING -> {
                RecoveryPolicy rp = task.getRecoveryPolicy();
                int attempt = rec.getAttempt() + 1;
                if (rp == RecoveryPolicy.FAIL || attempt >= task.getRetryPolicy().maxAttempts()) {
                    store.markFailed(rec.getId(), clock.instant(),
                            "interrupted by restart; recovery policy=" + rp);
                    log.info("Recovered RUNNING execution {} -> FAILED (policy={})", rec.getId(), rp);
                } else {
                    Instant retryAt = clock.instant().plus(task.getRetryPolicy().backoffAfter(attempt));
                    store.markRetryWait(rec.getId(), retryAt, "interrupted by restart");
                    queue.offer(newCmd(rec.getId(), task.getId(), rec.getFireTime(), retryAt));
                    log.info("Recovered RUNNING execution {} -> RETRY_WAIT at {}", rec.getId(), retryAt);
                }
            }
            case PENDING, QUEUED, RETRY_WAIT -> {
                Instant scheduledAt = rec.getNextRetryAt() != null ? rec.getNextRetryAt() : clock.instant();
                queue.offer(newCmd(rec.getId(), task.getId(), rec.getFireTime(), scheduledAt));
                log.info("Recovered {} execution {} -> requeued at {}", rec.getStatus(), rec.getId(), scheduledAt);
            }
            default -> {
                // SUCCEEDED / FAILED / SKIPPED / CANCELLED：终态，无需处理
            }
        }
    }

    private static String abbreviate(Throwable t) {
        String msg = t.toString();
        return msg.length() > 1900 ? msg.substring(0, 1900) : msg;
    }

    // ------------------------------------------------------------------
    // 队列元素
    // ------------------------------------------------------------------

    /** 延迟队列元素：携带执行 id、任务 id 与触发时刻。
     *  到期时刻在构造时按引擎时钟换算为 wall-clock 纳秒，
     *  保证延迟语义与调度时钟一致，不受时钟基准差异影响。 */
    public record ExecutionCmd(String executionId, String taskId, Instant fireTime,
                               Instant scheduledAt, long dueAtNanos) implements Delayed {

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(dueAtNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if (o instanceof ExecutionCmd other) {
                return scheduledAt.compareTo(other.scheduledAt);
            }
            return 0;
        }
    }
}
