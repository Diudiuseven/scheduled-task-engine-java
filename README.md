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

### 2.6 去重（Deduplication）

每个触发点唯一对应一条执行记录，唯一约束 `(task_id, fire_time)`。
同一触发点因重试、补跑、手动补偿被重复调度时：

- 只产生一次有效执行结果；
- 重复事件写入 `duplicate_log` 表，原因 `DUPLICATE`，可追溯。

**重复与拒绝的区分**：重复触发不改变既有执行记录，只在 `duplicate_log`
追加一条事件（`reason = DUPLICATE`，含检出时间）；而被拒绝的触发点会在
`execution_record` 中留下一条 `SKIPPED` 终态记录，`skip_reason` 标明具体原因
（`CATCHUP_WINDOW` / `OVERLAP` / `RATE_LIMITED` / `CONCURRENCY_LIMITED` /
`QUEUE_FULL`）。换言之：查“同一个触发点被调度了几次”看 `duplicate_log`，
查“某个触发点为什么没执行”看 `execution_record.skip_reason`。

### 2.7 重启恢复（Recovery）

状态持久化在 H2 文件库，重启后区分：

| 状态 | 恢复行为 |
| --- | --- |
| `SUCCEEDED` / `FAILED` / `SKIPPED` / `CANCELLED` | 终态，不处理 |
| `PENDING` / `QUEUED` / `RETRY_WAIT` | 重新入队，按计划时间执行 |
| `RUNNING` | 视为一次失败尝试：`RecoveryPolicy.RETRY`（默认，仍有重试次数则退避重试，否则 `FAILED`）或 `RecoveryPolicy.FAIL`（直接 `FAILED`） |

恢复后不重复执行、不漏触发点（最近触发点由持久化记录推导）。

### 2.8 限速与并发

- **全局限速**：令牌桶平滑触发速率（`engine.rate-limit`，每秒许可数，`<=0` 不限）；
- **单任务并发度**：`TaskDefinition.maxConcurrency`（默认 1）；
- 获取不到许可时排队等待（重入延迟队列），超过 `engine.acquire-timeout` 则拒绝，
  记录 `SKIPPED` / `RATE_LIMITED` 或 `CONCURRENCY_LIMITED`。

### 2.9 取消（Cancel）

`engine.cancel(executionId)`：

- 排队中：移出队列并标记 `CANCELLED`；
- 执行中：中断工作线程，处理器应响应中断并尽快退出（建议保证幂等性）；
- 取消后不留下半完成状态，状态一致落库。

### 2.10 状态流转规则（单调推进）

一次触发从登记到落库的全过程中，状态只允许按下表**单调推进**，任何并发的
排队/记账动作都不得把状态改回等待态：

| 当前状态 | 允许推进到 | 触发动作 |
| --- | --- | --- |
| `PENDING` | `QUEUED`、`RUNNING`、`RETRY_WAIT`、`SKIPPED`、`CANCELLED`、`FAILED` | 入队 / 恢复重入队 / 恢复重试 / 超限拒绝 / 取消 |
| `QUEUED` | `RUNNING`、`SKIPPED`、`CANCELLED`、`FAILED` | 开始执行 / 等待超时拒绝 / 取消 |
| `RETRY_WAIT` | `RUNNING`、`CANCELLED`、`FAILED` | 到点重试 / 取消 |
| `RUNNING` | `SUCCEEDED`、`RETRY_WAIT`、`FAILED`、`CANCELLED` | 完成 / 失败重试 / 失败终态 / 取消 |
| `SUCCEEDED` / `FAILED` / `SKIPPED` / `CANCELLED` | —（终态，不可再变） | — |

实现上，持久层的每一次状态变更都是**带前置状态守卫的条件更新**
（`UPDATE ... WHERE id = ? AND status IN (...)`）：

- 执行一旦进入 `RUNNING`/`SUCCEEDED`/`FAILED`/`CANCELLED`，滞后的
  `markQueued` 等记账动作不再生效，持久化结果与真实执行情况一致，
  重启后不会被当作未完成而补跑；
- `markRunning` 是执行的准入闸门：条件更新失败（记录已被并发取消/跳过/完成）
  时直接放弃执行，保证同一执行记录最多一次有效执行；
- 取消与完成并发时，先落库者获胜，另一方自动放弃，不会出现
  `CANCELLED` 被覆盖回 `SUCCEEDED` 或反之的半完成状态。

### 2.11 并发与容量边界

- 队列容量的“检查 + 入队”在同一临界区完成，多线程同时登记不会越过容量上限；
  超限触发点立即落库 `SKIPPED` / `QUEUE_FULL`，不丢失、可追踪；
- 单任务并发许可严格成对获取/释放，等待超时只拒绝该次执行
  （`SKIPPED` / `CONCURRENCY_LIMITED`），不会泄漏许可导致后续重叠执行；
- 延迟队列的到期时刻按引擎时钟计算并换算为 wall-clock，重试/重入队延迟
  不受时钟基准差异影响，引擎不会因记账顺序问题长时间空等。

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
- `StateMonotonicityTest`：状态单调性回归 —— 终态不被并发记账覆盖、
  并发登记与执行同时进行时完成记录保持 `SUCCEEDED`、同一触发点并发调度
  仅一次有效执行且重复事件落库、已完成触发点重启后不重跑、
  终态记录不进入恢复流程；
- `EngineBoundaryTest`：边界行为回归 —— 队列容量上限下并发登记不丢触发、
  超限拒绝并记录 `QUEUE_FULL`、触发与执行几乎同时发生时单任务不重叠、
  被跳过触发点各自留下 `CONCURRENCY_LIMITED` 记录、限流突发下全部触发
  在预算内完成不空等；
- `DstEngineTest` / `CronTriggerTest`：cron 与 DST 切换语义。

### 验证方法

1. `mvn test` 全量运行，连续多次均应稳定通过（已验证连续 3 次全绿）；
2. 状态单调性：任意时刻查询
   `SELECT status, COUNT(*) FROM execution_record GROUP BY status`，
   已完成的触发点（handler 已产生副作用）应对应 `SUCCEEDED`，
   不存在“执行完成但记录停留在 `PENDING`/`QUEUED`”的记录；
3. 重复与拒绝区分：重复触发查 `duplicate_log`（`reason = DUPLICATE`），
   被拒绝的触发点查 `execution_record` 中 `status = 'SKIPPED'` 的
   `skip_reason` 列；
4. 重启恢复：重启后 `findRecoverable` 只返回非终态记录，终态记录
   不会被重新执行（`StateMonotonicityTest` 中对应用例覆盖）。
