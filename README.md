# load-scenario-engine-go

一套可复现的负载场景执行引擎：以声明式场景描述为输入，结合可配置的负载曲线对
被测目标施加受控流量，运行结束后产出可校验、可合并的性能结论。

```
scenario/     声明式场景定义 + 确定性迭代器
loadcurve/    负载曲线（恒定 / 阶梯 / 尖峰 / 空闲回落）
engine/       执行引擎（并发上限、背压、取消、动态调整、检查点、熔断钩子）
stats/        可合并的延迟分布 / 吞吐 / 错误分类统计
fault/        故障与延迟注入、阈值熔断
checkpoint/   检查点持久化与恢复
cmd/loadengine  命令行入口
examples/run.json  示例配置
```

## 快速开始

```bash
go build ./cmd/loadengine
./loadengine -config examples/run.json
# 中断后从最近检查点恢复：
./loadengine -config examples/run.json -resume
```

运行测试：

```bash
go test ./...
go test -race ./...
```

## 场景描述格式（scenario）

场景是 JSON 中的一组有序步骤。相同种子（`seed`）与相同输入下，同一份场景
必然产生完全一致的动作序列（动作的 `key` 由 `(seed, seq)` 确定性导出）。

```json
"scenario": {
  "name": "checkout-mix",
  "seed": 42,
  "steps": [
    {"name": "browse",       "op": "read",  "weight": 40},
    {"name": "add-to-cart",  "op": "write", "weight": 12, "wait_before": "2ms"},
    {"name": "checkout",     "op": "txn",   "weight": 4,
      "on_failure": {"kind": "goto", "step": 3}},
    {"name": "rollback-cart","op": "delete","weight": 1}
  ]
}
```

| 字段 | 含义 |
| --- | --- |
| `name` / `op` | 步骤名与操作类型（`op` 用于故障注入规则匹配与报告） |
| `weight` | 该步骤发射的动作数（默认 1）。步骤按声明顺序执行 |
| `wait_before` | 动作下发前的确定性等待 |
| `wait_after` | 动作完成后的等待（占用 worker，可被取消打断） |
| `on_failure.kind` | `continue`（默认，继续下一步）、`abort`（终止场景）、`goto`（跳转到 `step` 指定的步骤索引，用于失败补偿分支） |

失败分支由生成器按动作序号顺序应用，因此动作流保持确定性。

## 负载曲线语义（loadcurve）

曲线由若干首尾相接的段组成，引擎每 tick（默认 50ms）按曲线目标值调整
worker 数：

| `kind` | 参数 | 语义 |
| --- | --- | --- |
| `constant` | `level` | 段内保持恒定并发 |
| `ramp` | `from`, `to` | 线性阶梯递增/递减 |
| `spike` | `level` | 尖峰：升到 `level`，段末回到基线 |
| `idle` | `level` | 空闲回落：降到 `level`（通常 0），段末回到基线 |

**连续性保证**：`spike`/`idle` 段在开头与结尾各用 10% 时长与基线做线性
过渡，`ramp` 终点与下一段起点衔接，因此 `TargetAt` 在任意切换处连续，
不会出现瞬时并发明显超出或低于配置目标。引擎侧另有第二道平滑：实际
worker 数每 tick 最多向目标靠近 `max(1, 25% × 目标)`，双向限幅。

## 执行语义（engine）

- **并发上限与背压**：动作由单一生成器产生，进入容量为 `queue_size` 的
  有界通道；worker 饱和时生成器阻塞，目标变慢不会无限堆积任务。
- **取消与超时**：`context` 取消、曲线结束、场景耗尽、`Stop()` 或熔断
  触发都会使引擎收敛：生成器停止、在途 handler 收到取消信号、所有
  goroutine 在 `Run` 返回前退出，资源释放。被取消的动作计入
  `cancelled`，**绝不计入成功**。
- **动态调整**：运行中可调用 `SetConcurrency(n)` 修改并发上限（`0` 表示
  暂停施压）、`Stop()` 提前停止。统计采集与调整解耦，调整前后的快照
  可通过 `stats.Merge` 合并且计数保持一致（`started == completed +
  cancelled`）。
- **统计**：固定桶指数直方图记录延迟，输出 p50/p95/p99/max、均值、吞吐、
  成功率与按错误消息分类的错误分布；快照可合并、可序列化。

## 故障注入与熔断（fault）

```json
"faults": [
  {"op": "txn",  "probability": 0.1, "latency": "20ms"},
  {"op": "read", "probability": 0.01, "error": "injected read failure"}
],
"breaker": {"error_rate": 0.5, "p99_latency": "2s", "min_samples": 50}
```

- 注入决策只取决于 `(seed, action.Seq)`，可复现；`op` 限定匹配的操作。
- 熔断器每 tick 评估一次快照：完成数达到 `min_samples` 后，错误率超过
  `error_rate` 或 p99 超过 `p99_latency` 即跳闸。跳闸后按策略停止施压
  （`stop`），引擎收敛并保存最终检查点以保留现场。

## 检查点与恢复（checkpoint）

设置 `checkpoint_path` 与 `checkpoint_interval` 后，引擎周期性并
在退出时原子地（临时文件 + rename）写入检查点：

```json
{"completed_actions": 1379, "elapsed": "...", "stats": {...}}
```

检查点中的 `completed_actions`（已连续完成前缀）与 `stats` 由同一份
原子进度指针导出，二者严格一致。`-resume` 重启后：迭代器确定性跳过
已完成前缀、统计以检查点为基座继续累加，**已完成的统计不会被重复
计算**；曲线从已消耗的偏移处继续。

> 注意：若场景中使用了 `on_failure: goto` 分支，恢复时迭代器按序重放
> 前缀，分支决策不重演，前缀之后的动作序列可能与原运行略有差异；
> 无 `goto` 分支时恢复是精确的。

## 配置项一览

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `scenario` | 必填 | 见“场景描述格式” |
| `curve` | 恒定上限 | 见“负载曲线语义” |
| `max_concurrency` | 1 | 并发硬上限（运行中可被 `SetConcurrency` 调整） |
| `queue_size` | = `max_concurrency` | 已生成未执行动作的有界队列长度 |
| `tick` | `50ms` | 控制循环间隔（曲线跟踪 / 熔断评估 / 平滑步进） |
| `checkpoint_path` | 无 | 检查点文件路径（不设置则不落盘） |
| `checkpoint_interval` | 无 | 周期检查点间隔 |
| `target.kind` | `sim` | `sim`（确定性模拟目标）或 `http` |
| `target.latency` / `target.error_rate` | | sim 目标的基准延迟与错误率 |
| `target.url` / `target.method` | | http 目标地址与方法 |
| `faults` | 无 | 故障/延迟注入规则 |
| `breaker` | 无 | 熔断阈值 |

## 输出报告

运行结束打印：完成/成功/失败/取消计数、成功率、吞吐、延迟均值与
p50/p95/p99/max、错误分类分布。相同种子、相同配置与相同目标行为下，
报告中的动作序列与统计完全一致，可用于跨运行的稳定对比。
