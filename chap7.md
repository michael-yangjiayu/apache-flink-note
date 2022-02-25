# 7. 有状态算子和应用

## 实现有状态函数

* **在 RuntimeContext 中声明键值分区状态**
  * 键值分区状态原语（必须作用在 KeyedStream）
    * `ValueState[T]`, `ListState[T]`, `MapState[K, V]`：支持读取、更新，支持 State.clear()
    * `ReducingState[T]`, `AggregatingState[I, O]`：add 后立即返回，支持 State.clear()
  * e.g. 相邻温度变化超过给定阈值时，发出警报
    * RichFunction：open 初始化，注册 StateDescriptor，状态引用对象 作为 普通成员变量
    * 单个 ValueState 简写：`mapWithState||flatMapWithState((IN, Option[S]))`
* **通过 ListCheckpointed 接口实现算子列表状态**
  * ListCheckpointed：列表结构允许对使用了就算子状态的函数修改并行度
    * snapshotState(checkpointId: Long, timestamp: Long): java.util.List\[T] // 返回状态快照列表
    * restoreState(java.util.List\[T] state): Unit // 恢复函数状态（启动/故障恢复）
  * e.g. 每个函数并行实例内，统计该分区数据超过某一阈值的温度值数目
* **使用联结的广播状态**
  * e.g. 一条规则流 + 一条需要应用这些规则的事件流
  * 步骤：
    * 调用 DataStream.broadcast() 方法创建 BroadcastStream 并提供 MapStateDescriptor 对象
    * 将 BroadcastStream 和一个 DataStream / KeyedStream 联结起来（connect）
    * 在联结后的数据流上应用函数： (Keyed)BroadcastProcessFunction
* **使用 CheckpointedFunction 接口**
  * CheckpointedFunction：用于指定由状态函数的最底层接口
    * initializeState()：创建并行实例时被调用
    * snapshotState()：在生成检查点之前调用
  * e.g. 分别利用键值分区状态和算子状态，统计每个键值分区和每个算子实例高温读数
* **接收检查点完成通知**
  * CheckpointListener.notifyCheckpointComplete(long checkpointid)
    * 会在 JobManager 将检查点注册为已完成时调用，但不保证每个完成的检查点都会调用

## 为有状态的应用开启故障恢复

* **检查点间隔：**较短的间隔会为常规处理带来较大的开销，但由于恢复时要重新处理的数据量较小，所以恢复速度会更快
* **其他配置选项：**一致性保障的选择、可同时生成的检查点数量、......

```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.enableCheckpointing(10000L) // 10s
```

## 确保有状态应用的可维护性

```scala
// 算子唯一标识
val alerts: DataStream[(String, Doouble, Double)] = keyedSensorData
    .flatMap(new TemperatureAlertFunction(1.1))
    .uid("TempAlert")

// 最大并行度（具有键值分区状态的算子）
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setMaxParallelism(512) // 应用级别
val alerts: DataStream[(String, Doouble, Double)] = keyedSensorData
    .flatMap(new TemperatureAlertFunction(1.1))
    .setMaxParallelism(1024) // 算子级别，覆盖应用级别
```

## 有状态应用的性能及鲁棒性

* **选择状态后端**

| 状态后端                | 特性        | 本地                  | 检查点            |
| ------------------- | --------- | ------------------- | -------------- |
| MemoryStateBackend  | 仅作开发/调试   | JVM 内存，延迟低，但OOM/GC  | JobManager 堆内存 |
| FsStateBackend      | 本地内存+故障容错 | JVM 内存，延迟低，但OOM/GC  | 远程持久化文件系统      |
| RocksDBStateBackend | 用于非常大的状态  | 本地 RocksDB 实例，序列化读写 | 远程持久化文件系统      |

* **选择状态原语**
  * MapState\[X, Y] 优于 ValueState\[HashMap\[X, Y]]
  * ListState\[X] 优于 ValueState\[List\[X]]
  * 建议每次函数调用只更新一次状态（检查点需要和函数调用同步）
* **防止状态泄露**
  * 清除无用状态（键值域变化）：注册针对未来某个事件点的计时器（窗口Trigger、处理函数）

## 更新有状态应用

> 生成保存点，停用该应用，重启新版本

* **保持现有状态更新应用**
* **从应用中删除状态**
* **修改算子的状态**
  * (√) 通过更改状态的数据类型，e.g. ValueState\[Int] => ValueState\[Double]，Avro 类型支持
  * (x) 通过更改状态原语类型，e.g. ValueState\[List\[String]] => ListState\[String]，不支持

## 查询式状态

* **服务进程**：QueryableStateClient, QueryableStateClientProxy, QueryableStateServer
* **对外暴漏可查询式状态**
  * 通用方法：StateDescriptor.setQueryable(String)
  * 简便方法：数据流.keyBy().setQueryable(String) 添加可查询式状态的数据汇
* **从外部系统查询状态**
  * QueryableStateClient(tmHostname, proxyPort).getKvState()
  * e.g. Flink 应用状态查询仪表盘
