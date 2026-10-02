# Scheduled Task Engine

面向周期任务的自动触发与执行引擎。任务按周期反复运行，失败后自动退避重试，
服务重启或长时间停机后不会丢失触发点。

代码位于 `com.github.diudiuseven.scheduledtaskenginejava.engine` 包：

```
engine/
├── trigger/    触发定义（固定间隔 / cron 日历表达式，均携带时区）
├── policy/     既定策略（重试、补跑、重叠、限流）
├── model/      任务定义与执行记录
├── store/      状态持久化（内存 / JSON 文件）
└── core/       调度引擎、限流器、事件监听
```

## 快速开始

```java
ExecutionStore store = new FileExecutionStore(Path.of("data/executions.json"));
SchedulerEngine engine = new SchedulerEngine(EngineConfig.defaults(), store, Clock.systemUTC());

TaskDefinition task = TaskDefinition.builder("daily-report",
                new CronTrigger("0 0 9 * * *", ZoneId.of("Asia/Shanghai")))
        .retryPolicy(new RetryPolicy(3, Duration.ofSeconds(10), 2.0, Duration.ofMinutes(5)))
        .misfirePolicy(new MisfirePolicy(Duration.ofHours(2)))
        .overlapPolicy(OverlapPolicy.SKIP)
        .rejectPolicy(RejectPolicy.QUEUE)
        .maxConcurrency(1)
        .build();

engine.register(task, execution -> {
    // 业务逻辑；抛出异常即视为失败并进入重试流程
});
engine.start();
```

## 调度语义

### 触发点与唯一性

- 每个触发点以 `(taskId, fireTime)` 唯一标识，对应一条 `TaskExecution` 记录。
- 同一触发点因重试、补跑或手动触发被重复调度时，只产生一次有效执行；
  重复触发通过 `EngineListener.onDuplicate` 识别并记录。

### 触发定义与时区

- `FixedIntervalTrigger(anchor, interval, zone)`：从锚点按绝对时间等间隔推进，
  不受夏令时影响。
- `CronTrigger(cron, zone)`：在**本地时间空间**计算下一个触发时刻后再解析回时区：
  - 春进（时间跳空）：不存在的本地时刻顺延 gap 长度（如 02:30 → 03:30），当天不丢触发；
  - 秋回（时间回拨）：重复的本地时刻只取第一次出现，当天不重复触发。
  - 因此夏令时切换前后同一时刻的触发次数**确定且可复现**。

### 停机补跑（Misfire）

- 重启后从该任务已知的最后一个触发点推进到当前时间，补算错过的触发点。
- 落在补跑窗口 `[now - catchUpWindow, now]` 内的触发点被补跑；
  超出窗口的触发点记录为 `SKIPPED`，原因 `out-of-misfire-window`。

### 重叠执行（Overlap）

- 任一时刻同一任务最多 `maxConcurrency` 个有效执行（默认 1）。
- 执行耗时超过周期时按 `OverlapPolicy` 处理：
  - `SKIP`：新触发点记录为 `SKIPPED`，原因 `overlap`；
  - `QUEUE`：新触发点排队等待，许可可用后串行执行。

### 失败重试（Retry）

- 执行体抛出异常即视为失败，按 `RetryPolicy` 退避重试：
  `delay = initialDelay * backoffMultiplier^(attempt-1)`，封顶 `maxDelay`（0 表示不封顶）。
- 重试通过延迟调度重新进入派发队列，**不阻塞**其它任务的正常触发。
- 重试次数耗尽后记录为 `FAILED`，原因携带异常信息。

### 速率与并发限制

- 引擎级限制见 `EngineConfig`：全局触发速率（令牌桶）与全局并发度（信号量）。
- 超限时按任务级 `RejectPolicy` 处理：
  - `QUEUE`：进入等待队列，下个 tick 重试派发；
  - `REJECT`：记录为 `SKIPPED`，原因 `rate-limited` / `global-concurrency-limited`。

### 持久化与重启恢复

- 所有状态变更写入 `ExecutionStore`（`FileExecutionStore` 以 JSON 原子落盘）。
- 重启后可区分：已完成（`SUCCESS`）、失败（`FAILED`）、在途（`RUNNING`）、
  等待中（`PENDING`）、已跳过（`SKIPPED`）、已取消（`CANCELLED`）。
- 恢复策略：`RUNNING`/`PENDING` 记录重新进入调度（`recovered-from-restart`），
  终态记录绝不重跑；基于 `(taskId, fireTime)` 去重，不重不漏。
- 优雅关闭（`close()`）：进行中的执行被中断并落库为 `PENDING(shutdown-interrupted)`，
  不留下半完成状态，下次启动继续；因关闭产生的中断**不会**消耗重试次数。
- `cancel(taskId, fireTime)` 可取消等待中或进行中的执行，取消后状态为终态
  `CANCELLED`，执行体返回也不会改写状态。

## 配置项

### EngineConfig（引擎级）

| 配置 | 含义 | 默认值 |
|---|---|---|
| `tickInterval` | 调度 tick 间隔 | 100ms |
| `maxTriggersPerSecond` | 全局触发速率上限（令牌桶容量=速率） | 100 |
| `maxGlobalConcurrency` | 全局并发执行上限 | 16 |

### TaskDefinition（任务级）

| 配置 | 含义 | 默认值 |
|---|---|---|
| `trigger` | 触发定义（固定间隔 / cron，携带时区） | 必填 |
| `retryPolicy` | `maxAttempts` / `initialDelay` / `backoffMultiplier` / `maxDelay` | 不重试 |
| `misfirePolicy` | 补跑窗口 `catchUpWindow` | 1 小时 |
| `overlapPolicy` | 重叠处理：`SKIP` / `QUEUE` | `SKIP` |
| `rejectPolicy` | 超限处理：`QUEUE` / `REJECT` | `QUEUE` |
| `maxConcurrency` | 单任务并发度上限 | 1 |

## 测试

```bash
mvn test
```

覆盖场景：

- `TriggerTest`：固定间隔推进、cron 触发、夏令时春进/秋回的确定性；
- `MisfireTest`：停机后补跑窗口内触发点、窗口外触发点记录为跳过；
- `OverlapTest`：执行超时重叠时 SKIP/QUEUE 两种策略，任一时刻最多一个有效执行；
- `RetryTest`：退避重试至成功、重试耗尽转 FAILED、重试不阻塞其它任务；
- `DuplicateTriggerTest`：重复触发只产生一次有效执行并被识别记录；
- `RestartRecoveryTest`：状态持久化、重启恢复在途执行、终态不重跑；
- `ConcurrencyLimitTest`：速率/并发超限的排队与拒绝、取消不留半完成状态。
