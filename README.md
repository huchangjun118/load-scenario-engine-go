# load-scenario-engine：可复现负载场景执行引擎

一套以**声明式场景**为输入、由**可配置负载曲线**驱动的本地负载执行引擎。
引擎对被测目标施加受控流量，运行结束后产出可校验、可合并的性能结论。

- 同一份场景在相同种子下产生**完全一致**的动作序列（确定性 RNG 派生）。
- 支持恒定、阶梯递增、尖峰、空闲回落及**组合曲线**，曲线切换处平滑过渡，无瞬时并发尖峰。
- 明确的并发上限与背压：目标变慢时任务不堆积；取消/超时使在途动作尽快收敛并释放资源。
- 可复现的延迟分布（p50/p90/p99/p999）、吞吐与错误分类；未完成/已取消动作不计入成功。
- 运行中可动态调整并发上限或提前停止，统计一致性不被破坏，调整前后结果可合并、可解释。
- 支持故障与延迟扰动注入、阈值熔断；触发后停止施压并保留现场（检查点），重启后从检查点继续，**不重复计算**已完成统计。

## 目录

- [快速开始](#快速开始)
- [场景描述格式](#场景描述格式)
- [负载曲线语义](#负载曲线语义)
- [动作类型](#动作类型)
- [失败分支](#失败分支)
- [配置项](#配置项)
- [故障注入](#故障注入)
- [阈值熔断](#阈值熔断)
- [动态调整与提前停止](#动态调整与提前停止)
- [检查点与恢复](#检查点与恢复)
- [运行结果说明](#运行结果说明)
- [确定性保证](#确定性保证)
- [架构与测试](#架构与测试)

## 快速开始

```bash
go build -o loadgen ./cmd/loadgen

# 对本地目标运行示例场景（2 秒）
./loadgen -scenario examples/quick.json -duration 2s

# 带熔断、故障注入与检查点
./loadgen -scenario examples/scenario.json \
  -breaker -breaker-threshold 0.5 -breaker-min-samples 20 \
  -fault-prob 0.01 -latency-min 10ms -latency-max 100ms \
  -checkpoint /tmp/ckpt.json -report /tmp/report.json

# 中断后从检查点恢复（统计连续，不重复计数）
./loadgen -scenario examples/scenario.json -checkpoint /tmp/ckpt.json -resume
```

## 场景描述格式

场景为 JSON 文件，顶层字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `name` | string | 场景名称，用于报告 |
| `seed` | int64 | 随机种子；相同种子 + 相同场景 => 相同动作序列 |
| `curve` | object | 负载曲线定义，见[负载曲线语义](#负载曲线语义) |
| `steps` | array | 步骤列表，至少一个 |

每个步骤：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | string | 步骤唯一标识，用于 `on_error.goto` 跳转 |
| `action` | object | 动作定义，见[动作类型](#动作类型) |
| `wait` | duration | 动作执行前的等待（步骤间等待），如 `"100ms"` |
| `on_error` | object | 失败分支，见[失败分支](#失败分支) |

`duration` 字段接受 Go 风格时长字符串（`"100ms"`、`"2s"`）或数字秒。

示例：

```json
{
  "name": "demo",
  "seed": 42,
  "curve": { "type": "constant", "level": 4 },
  "steps": [
    {
      "id": "req",
      "action": { "type": "http", "method": "GET", "url": "http://127.0.0.1:8080/" },
      "wait": "100ms"
    },
    {
      "id": "think",
      "action": { "type": "sleep", "duration": "200ms" }
    }
  ]
}
```

虚拟用户（VU）按步骤顺序循环执行；到达末尾后回到第一步，迭代数加一。

## 负载曲线语义

曲线把**已运行时间**映射为**目标并发数**（虚拟用户数）。引擎每 `tick-interval`
（默认 200ms）重新求值并调整 worker 数量：超过目标则取消多余 worker（其在途动作
被取消并记录为 `canceled`），不足则补充。

### 恒定 `constant`

```json
{ "type": "constant", "level": 10 }
```

### 阶梯递增 `step`

`base` 为初始水平，`steps` 为按时间排序的跳变点列表：

```json
{
  "type": "step",
  "base": 5,
  "steps": [
    { "at": "10s", "level": 20 },
    { "at": "20s", "level": 40 }
  ]
}
```

### 尖峰 `spike`

在 `[start, end)` 窗口内把并发提升到 `level`，窗口外回落到 `base`：

```json
{ "type": "spike", "base": 5, "level": 80, "start": "2s", "end": "4s" }
```

### 空闲回落 `idle`

在 `[start, end)` 窗口内把并发置零（停止施压），窗口外恢复 `base`：

```json
{ "type": "idle", "base": 10, "start": "0s", "end": "3s" }
```

### 求和 `sum`

把多条曲线叠加（例如基准 + 周期性尖峰）：

```json
{
  "type": "sum",
  "curves": [
    { "type": "constant", "level": 5 },
    { "type": "spike", "level": 20, "start": "10s", "end": "12s" }
  ]
}
```

### 组合 `composite`

按顺序播放多个分段，分段之间通过 `transition` 窗口做 **smoothstep 交叉淡入淡出**，
保证曲线切换处目标并发连续可导，不出现瞬时尖峰或失压：

```json
{
  "type": "composite",
  "segments": [
    {
      "curve": { "type": "constant", "level": 5 },
      "duration": "10s",
      "transition": "2s"
    },
    {
      "curve": { "type": "step", "base": 5, "steps": [{ "at": "0s", "level": 20 }] },
      "duration": "10s",
      "transition": "3s"
    }
  ]
}
```

- 首个分段的过渡窗口从 0 淡入（启动爬坡）。
- 过渡窗口内：`prev_level * (1-frac) + cur_level * frac`，
  `frac = smoothstep(local/transition)`。
- 超出最后一个分段后保持末段最终水平。

### 限时

CLI 的 `-duration` 会把曲线包一层 `Limited`：超出时长后目标置零，引擎结束运行。

## 动作类型

| 类型 | 字段 | 说明 |
| --- | --- | --- |
| `http` | `method`（默认 GET）、`url`、`body` | 发起 HTTP 请求；4xx/5xx 记为失败并分类为 `http_4xx`/`http_5xx`；网络错误记为 `error` |
| `sleep` | `duration` | 休眠指定时长（可被取消） |
| `log` | `body` | 打印一行日志（不接触目标） |

动作执行受上下文约束：

- `-action-timeout`：单个动作超时，超时记为 `timeout`。
- 取消（SIGINT/熔断/停止）会取消在途动作，记为 `canceled`，**不计入成功**。

自定义动作可通过 `engine.RegisterAction` 注册（测试与扩展使用）。

## 失败分支

步骤可声明 `on_error`，动作失败时采取以下策略之一：

| action | 行为 |
| --- | --- |
| `continue` | 继续执行下一步骤（失败已统计） |
| `goto` | 跳转到 `goto` 指定的步骤 id |
| `stop` | 停止当前 VU |

未声明 `on_error` 时，失败后继续下一步骤。

## 配置项

| CLI 标志 | 配置字段 | 默认 | 说明 |
| --- | --- | --- | --- |
| `-scenario` | — | 必填 | 场景 JSON 文件路径 |
| `-duration` | — | 曲线定义/60s | 覆盖运行时长 |
| `-max-concurrency` | `MaxConcurrency` | 0（不限） | 并发硬上限；目标变慢时任务不堆积，在途动作数永不超过此值 |
| `-tick-interval` | `TickInterval` | 200ms | 曲线重评估间隔 |
| `-action-timeout` | `ActionTimeout` | 0（不限制） | 单动作超时 |
| `-shutdown-timeout` | `ShutdownTimeout` | 10s | 停止时在途动作的最大收敛等待 |
| `-seed` | `Seed` | 场景 seed | 随机种子覆盖 |
| `-fault-prob` | `FaultProbability` | 0 | 单动作故障注入概率（0..1） |
| `-latency-min` | `LatencyMin` | 0 | 注入延迟下限 |
| `-latency-max` | `LatencyMax` | 0 | 注入延迟上限 |
| `-breaker` | `BreakerEnabled` | false | 启用阈值熔断 |
| `-breaker-threshold` | `BreakerThreshold` | 0.5 | 触发熔断的错误率（0..1） |
| `-breaker-min-samples` | `BreakerMinSamples` | 20 | 触发前的最小样本数 |
| `-breaker-window` | `BreakerWindow` | 30s | 错误率统计窗口 |
| `-checkpoint` | `CheckpointPath` | 空 | 检查点文件路径 |
| `-resume` | `Resume` | false | 从检查点恢复 |
| `-report` | — | stdout | 报告输出路径 |

## 故障注入

注入由确定性 RNG 驱动（与场景同源种子），因此可复现：

- **延迟扰动**：每个动作额外增加 `[latency-min, latency-max]` 内的随机延迟。
- **故障注入**：每个动作以 `fault-prob` 概率直接返回 `injected fault` 错误，
  错误分类为 `fault`。

```bash
./loadgen -scenario examples/quick.json -duration 10s \
  -fault-prob 0.02 -latency-min 50ms -latency-max 200ms
```

## 阈值熔断

启用后，引擎在滑动窗口内统计错误率（失败 + 超时）：

- 样本数达到 `breaker-min-samples`；
- 且错误率 >= `breaker-threshold`；

则触发熔断：立即取消所有在途动作、保存检查点并以退出码 3 结束，
`stop_reason` 为 `breaker`。在途资源快速收敛，现场保留在检查点中。

## 动态调整与提前停止

- **并发调整**：`Engine.SetConcurrency(n)` 可在运行中覆盖并发上限；`n<=0`
  解除覆盖、回到曲线驱动。调整立即生效，统计始终在互斥锁保护下采集，
  调整前后结果可通过快照合并。
- **提前停止**：`Engine.Stop()` 或向进程发送 `SIGINT`/`SIGTERM`：
  引擎取消在途动作、保存检查点后退出，`stop_reason` 为 `stopped`。

## 检查点与恢复

检查点（JSON）包含：

- 场景名、种子、已运行曲线时间 `elapsed`；
- 每个 VU 的可恢复状态（迭代数、步骤序号）；
- 统计快照（计数器 + 直方图）。

恢复时（`-resume`）：

- VU 从上次的步骤/迭代继续；
- 统计快照**合并**进当前统计，不重新计数；
- 曲线从 `elapsed` 继续，不重复已经历的曲线段。

```bash
# 运行 10 秒，1 秒后中断
./loadgen -scenario examples/quick.json -duration 10s -checkpoint /tmp/ckpt.json &
LPID=$!; sleep 1; kill -INT $LPID

# 从 0.8s 处继续，再跑约 9.2s，统计连续
./loadgen -scenario examples/quick.json -duration 10s -checkpoint /tmp/ckpt.json -resume
```

## 运行结果说明

报告为 JSON：

```json
{
  "scenario": "quick",
  "seed": 7,
  "started": 3436,
  "success": 3436,
  "failed": 0,
  "canceled": 0,
  "timed_out": 0,
  "errors": {},
  "throughput_per_sec": 3436.0,
  "elapsed": 1000000000,
  "quantiles": { "p50": 185000, "p90": 245000, "p99": 380000, "p999": 890000 },
  "stop_reason": "completed"
}
```

- `started`：开始执行的动作总数；`success`/`failed`/`canceled`/`timed_out` 为结果分类。
- 正常结束（`completed`）时引擎会**排空**在途动作：让当前动作执行完、不再开始新动作，因此在途动作计入成功，`canceled` 为 0。
- 提前停止 / 熔断 / 外部取消时，在途动作被取消并记为 `canceled`，**不计入成功**。
- `errors`：错误分类计数（`timeout`/`canceled`/`http_4xx`/`http_5xx`/`fault`/`error`）。
- `throughput_per_sec`：成功动作的吞吐（成功数 / 运行秒数）。
- `quantiles`：基于对数桶直方图（1µs–90s）的分位数估计。
- `stop_reason`：`completed`（正常结束）、`stopped`（提前停止）、`breaker`（熔断）、`canceled`（外部取消）。

## 确定性保证

- 每个 VU 的随机流由 `DeriveSeed(seed, vuID, iteration)` 派生（splitmix64 混合），
  与调度时序无关：同一种子 + 同一场景 => 每个 VU 的动作序列完全一致。
- 故障/延迟注入消耗同一随机流，序列同样确定。
- 检查点恢复后，VU 从保存的迭代/步骤继续，随机流与首次运行无缝衔接。

## 架构与测试

```
scenario/   场景定义（JSON 解析、校验、确定性种子派生）
curve/      负载曲线（constant/step/spike/idle/sum/composite，smoothstep 过渡）
engine/     执行引擎
  actions.go      动作实现（http/sleep/log）与注册表
  engine.go       worker 池、控制器、背压、动态并发、熔断、检查点恢复
  stats.go        对数桶直方图、分位数、可合并快照
  checkpoint.go   检查点原子写入/加载
  inject.go       确定性故障/延迟注入
cmd/loadgen/ CLI 入口
```

测试：

```bash
go test ./...          # 全部测试
go test -race ./...    # 并发安全
```

覆盖场景：

- **曲线切换**：`curve/curve_test.go` 验证恒定/阶梯/尖峰/空闲语义，
  以及组合曲线过渡处每 10ms 采样的并发跳变不超过 0.5（平滑）。
- **背压**：`engine/engine_test.go::TestBackpressure` 验证目标阻塞时
  在途数永不超过硬上限、放开后正常完成。
- **取消**：`TestCancel` 验证取消后在途动作快速收敛，取消数不计入成功。
- **熔断**：`TestBreaker` 验证持续失败触发熔断并以 `breaker` 结束。
- **检查点恢复**：`TestCheckpointResume` 验证中断后恢复统计连续、不重复计数，
  且 VU 状态写入检查点。
- **确定性**：`TestDeterminism` 验证两次运行每个 VU 的随机序列完全一致。
- **动态并发**：`TestDynamicConcurrency` 验证运行中调整上限立即生效且不超限。
- **超时**：`TestActionTimeout` 验证单动作超时收敛并分类。
