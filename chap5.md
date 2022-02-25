# 5. DataStream API

## 转换操作

* **基本转换**
  * map(T): O
  * filter(T): Boolean
  * flatMap(T, Collector\[O]): Unit
* **基于 KeyedStream 的转换**
  * keyBy：DataStream => KeyedStream
  * 滚动聚合：sum, min, max, minBy, maxBy
  * reduce(T, T): T
* **多流转换**
  * union: FIFO
  * connect: DataStream => ConnectedStream
    * coMap, coFlatMap
    * 结合 keyBy 和 broadcast
    * e.g. 当温度>threshold 且 烟雾指数很高时，发出火灾警报
  * split(OutputSelector\[IN]) => SplitStream
    * select(IN): Iterable\[String]
* **分发转换**
  * 随机：shuffle
  * 轮流：rebalance
  * 重调：rescale（部分后继任务）
  * 广播：broadcast
  * 全局：global（下游算子的第一个并行任务）
  * 自定义：partitionCustom(Partitioner)

## 设置并行度

* 覆盖环境默认并行度：无法通过提交客户端控制应用并行度
* 覆盖算子默认并行度

## 类型

* 支持的数据类型
  * Java & Scala 原始类型
  * Java & Scala 元组
  * Scala 样例类
  * POJO
  * 数组、列表、映射、枚举以及其他特殊类型
* 为数据类型创建类型信息  val stringType: TypeInformation\[String] = Types.STRING
* 显式提供类型信息：ResultTypeQueryable

## 定义键值和引用字段

* 字段位置（Tuple）
* 字段表达式（Tuple, POJO, case class）
* 键值选择器：getKey(IN): KEY

## 实现函数

* 函数类（或匿名类）
* Lambda 函数
* 富函数：RichMapFunction、RichFlatMapFunction
  * open：初始化方法，常用于一次性设置工作，如连接外部系统
  * close：终止方法，常用于清理和释放资源

## 导入外部和 Flink 依赖

* \[推荐] 将全部依赖打进应用的 JAR 包
* 将依赖的 JAR 包放在设置 Flink 的 ./lib 目录中
