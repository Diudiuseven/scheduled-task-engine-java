# 周期任务触发执行引擎（scheduled-task-engine）

面向周期任务的自动触发与执行能力：按固定间隔 / 日历表达式触发，失败自动退避重试，
服务重启或长时间停机后不丢失触发点，触发与执行状态持久化，支持限速、并发控制与取消。

## 1. 快速开始

```bash
mvn spring-boot:run
```

应用启动后会注册两个示例任务（`engine.demo.enabled=false` 可关闭）：

- `demo-interval`：固定间隔 10s，UTC；
- `demo-cron`：cron `0 * * * *`，America/New_York。

自定义任务：实现 `TaskHandler`（或 lambda），通过 `TaskDefinition.builder(...)` 注册即可。

## 2. 调度语义

### 2.1 触发定义（TriggerDefinition）

| 类型 | 说明 |
| --- | --- |
| `IntervalTrigger` | 固定间隔，基于绝对瞬时（`Duration`）计算，**不受夏令时影响**。 |
| `CronTrigger` | 5 字段日历表达式 `分 时 日 月 周`（秒恒为 0），**必须携带时区**。 |

支持的 cron 语法：`*`、`?`、`a,b,c`、`a-b`、`a-b/n`、`*/n`、`n`，
月份/星期支持 `JAN..DEC` / `SUN..SAT` 名称；日与周同时限制时取**或**（标准 cron 语义）。

### 2.2 夏令时（DST）语义 —— 确定且可复现

触发点以 `ZonedDateTime`（带偏移）解析：

- **春季跳变（gap）**：本地时间在跳变窗口内不存在，触发点**顺延**为跳变结束后的第一个有效时刻。
  例：Europe/Berlin `30 2 * * *`，2024-03-31 的 `02:30` 不存在（02:00→03:00），触发点顺延到 `03:00`。
- **秋季重叠（overlap）**：同一本地时间对应两个偏移，**两个瞬时各触发一次**。
  例：2024-10-27 的 `02:30` 出现两次（`+02:00` 与 `+01:00`），各触发一次。

因此春秋切换日的触发次数由表达式唯一确定，可复现。

### 2.3 补跑（CatchUp）

停机或长时间停止后重启，引擎从持久化的最近触发点继续计算：

- 窗口 `(now - window, now]` 内**错过的触发点补算执行**；
- 超出窗口的触发点记录为 `SKIPPED`，原因 `CATCHUP_WINDOW`。

窗口可配置（`CatchUpPolicy.of(window)`），也可关闭（`CatchUpPolicy.disabled()`）。

### 2.4 重叠（Overlap）

同一任务上一次执行未结束时，新触发点的处理策略：

- `OverlapPolicy.SKIP`（默认）：跳过本次触发点，记录 `SKIPPED` / `OVERLAP`；
- `OverlapPolicy.QUEUE`：排队等待，上一次结束后按顺序补执行（队列有界，溢出记 `QUEUE_FULL`）。

任一时刻同一任务最多一个有效执行（由单任务信号量保证）。

### 2.5 失败重试（Retry）

执行抛异常时按退避策略重试，重试进入**延迟队列**，不阻塞其它任务：

- 退避：`min(base * factor^(attempt-1), max) * (1 ± jitter)`；
- `maxAttempts` 为最大执行次数（含首次），用尽后标记 `FAILED`；
- 重试是同一条执行记录的多次尝试（`attempt` 递增），**不产生新记录**。

### 2.6 状态流转（单调、不回退）

每条执行记录的状态机：

```
PENDING -> QUEUED -> RUNNING -> SUCCEEDED
                     |   └---> RETRY_WAIT -> (重新入队) -> RUNNING
                     |   └---> FAILED（重试用尽 / 恢复策略 FAIL）
                     └---> SKIPPED（限流/并发/队列/补跑窗口/重叠）
任一非终态 -> CANCELLED（取消）
```

**状态单调保证**：所有状态推进都是带前置条件的原子更新
（`UPDATE ... WHERE id=? AND status IN (...)`），只会向前推进、绝不回退。
即使触发登记与执行开始几乎同时发生、或多个线程并发操作同一记录：

- 执行已进入 `RUNNING`/`SUCCEEDED` 后，任何滞后的排队/记账动作都无法把状态改回
  `QUEUED`/`PENDING`，持久化结果与真实执行情况一致；
- worker 只有在把记录从 `PENDING/QUEUED/RETRY_WAIT` 原子推进到 `RUNNING` 成功后
  才会真正调用处理器，否则放弃本次执行 —— 同一执行记录**最多产生一次有效执行**；
- 记录被并发取消时，执行完成后的成功记账会被拒绝，状态保持 `CANCELLED`。

### 2.7 去重与拒绝原因（Deduplication & SkipReason）

每个触发点唯一对应一条执行记录，唯一约束 `(task_id, fire_time)`。
同一触发点因重试、补跑、手动补偿、并发登记被重复调度时，只产生一次有效执行结果。

**重复**与**其它原因拒绝**分开记录、可区分：

| 类别 | 记录位置 | 取值 |
| --- | --- | --- |
| 重复触发（该触发点已存在执行记录） | `duplicate_log` 表 | `DUPLICATE` |
| 触发点本身被拒绝/跳过 | `execution_record.skip_reason` | `CATCHUP_WINDOW` / `OVERLAP` / `RATE_LIMITED` / `CONCURRENCY_LIMITED` / `QUEUE_FULL` / `RECOVERY_FAILED` / `CANCELLED_BY_USER` |

即：查 `duplicate_log` 可回答“这个触发点被多调度了几次”，
查 `execution_record.skip_reason` 可回答“这次执行为何没有发生”。

### 2.8 重启恢复（Recovery）

状态持久化在 H2 文件库，重启后区分：

| 状态 | 恢复行为 |
| --- | --- |
| `SUCCEEDED` / `FAILED` / `SKIPPED` / `CANCELLED` | 终态，**绝不重新捡起执行** |
| `PENDING` / `QUEUED` / `RETRY_WAIT` | 重新入队，按计划时间执行 |
| `RUNNING` | 视为一次失败尝试：`RecoveryPolicy.RETRY`（默认，仍有重试次数则退避重试，否则 `FAILED`）或 `RecoveryPolicy.FAIL`（直接 `FAILED`） |

恢复集合与真实在途状态一致：只有未到终态的记录进入恢复流程，
已完成的触发点重启后不会补跑、副作用不会重复。

### 2.9 限速与并发

- **全局限速**：令牌桶平滑触发速率（`engine.rate-limit`，每秒许可数，`<=0` 不限）；
- **单任务并发度**：`TaskDefinition.maxConcurrency`（默认 1）；
- 获取不到许可时排队等待（非阻塞重入延迟队列，worker 不会被挂起空等），
  超过 `engine.acquire-timeout` 则拒绝，记录 `SKIPPED` / `RATE_LIMITED` 或 `CONCURRENCY_LIMITED`；
- 先预检全局限速（不消费许可）、再占用单任务并发许可、最后消费限速许可，
  保证拒绝原因归因准确（限速导致的拒绝不会误记为并发超限）；
- 队列容量有界（`engine.queue-capacity`），溢出触发点记录 `SKIPPED` / `QUEUE_FULL`，不静默丢失。

### 2.10 取消（Cancel）

`engine.cancel(executionId)`：

- 排队中：移出队列并标记 `CANCELLED`；
- 执行中：中断工作线程，处理器应响应中断并尽快退出（建议保证幂等性）；
- 取消后不留下半完成状态，状态一致落库。

## 3. 配置项（application.properties）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `engine.worker-pool-size` | 4 | 工作线程数 |
| `engine.rate-limit` | 10 | 全局限速（次/秒），`<=0` 不限 |
| `engine.queue-capacity` | 200 | 执行队列容量（有界） |
| `engine.tick-interval` | 200ms | 触发调度轮询间隔 |
| `engine.acquire-timeout` | 5s | 排队/并发等待超时 |
| `engine.default-catch-up-window` | 1h | 默认补跑窗口 |
| `engine.default-max-attempts` | 3 | 默认重试次数（含首次） |
| `engine.default-backoff-base` | 1s | 默认退避基数 |
| `engine.default-backoff-max` | 30s | 默认退避上限 |
| `engine.default-backoff-factor` | 2.0 | 默认退避因子 |
| `engine.default-jitter-ratio` | 0.2 | 默认抖动比例 |
| `engine.db-path` | ./data/scheduled-task-engine | H2 持久化文件路径 |
| `engine.demo.enabled` | true | 是否注册示例任务 |

任务级策略通过 `TaskDefinition.builder(...)` 覆盖：

```java
TaskDefinition.builder("my-task", trigger, handler)
    .overlapPolicy(OverlapPolicy.QUEUE)
    .catchUpPolicy(CatchUpPolicy.of(Duration.ofMinutes(10)))
    .retryPolicy(RetryPolicy.of(5, Duration.ofSeconds(1), Duration.ofMinutes(1), 2.0, 0.2))
    .recoveryPolicy(RecoveryPolicy.RETRY)
    .maxConcurrency(1)
    .startTime(Instant.now())
    .build();
```

## 4. 持久化表

- `execution_record`：执行记录（`task_id` + `fire_time` 唯一），状态、尝试次数、
  计划/开始/完成时间、下次重试时间、错误信息、跳过原因；
- `duplicate_log`：重复触发事件。

时间以 UTC 存储。

## 5. 测试

```bash
mvn test
```

覆盖场景：

- `CatchUpTest`：补跑窗口内补算、窗口外跳过（含关闭补跑）；
- `OverlapTest`：超时重叠跳过、执行区间不重叠；
- `RetryTest`：退避重试、重试不阻塞其它任务、用尽后失败；
- `DuplicateTest`：重复触发只执行一次、重试不产生新记录；
- `RestartRecoveryTest`：在途执行恢复（RETRY/FAIL）、重启不重不漏；
- `ConcurrencyTest`：限速平滑、限速拒绝、单任务并发串行化、取消执行中/排队中；
- `QueueFullTest`：队列溢出拒绝；
- `RaceRegressionTest`（时序回归）：
  - 多线程并发登记同一触发点与执行同时进行 → 恰好一次有效执行，其余记 `DUPLICATE`；
  - 触发点执行完成后重启 → 不重跑、副作用不重复，再次登记记为重复；
  - 终态（成功/失败/取消）记录重启后不被恢复捡起；
  - 大量任务同时到点、数量接近队列容量 → 不丢触发、不空等，全部到达终态且各执行一次；
- `DstEngineTest` / `CronTriggerTest`：cron 与 DST 切换语义。

验证方式：整套测试可连续多次全量运行（`mvn test`），应全部稳定通过。
