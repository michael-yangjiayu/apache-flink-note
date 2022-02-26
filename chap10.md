# 10. Flink 和流式应用运维

## 运行并管理流式应用

### 保存点

```bash
/savepointes/savepoint-:shortjobid-:savepointid/_metadata # 元数据文件
/savepointes/savepoint-:shortjobid-:savepointid/:xxx      # 存储的算子状态
```

### 通过命令行客户端管理应用

```bash
# 启动应用
./bin/flink run ~/myApp.jar [args]                 # 启动应用
./bin/flink run -d ~/myApp.jar                     # 分离模式
./bin/flink run -p 16 ~/myApp.jar                  # 指定默认并行度
./bin/flink run -c my.app.MainClass ~/myApp.jar    # 指定入口类
./bin/flink run -m myMasterHost:9876 ~/myApp.jar   # 提交到特定的主进程

# 列出正在运行的应用
./bin/flink list -r

# 生成和清除保存点
./bin/flink savepoint <jobId> [savepointPath]
./bin/flink savepoint -d <savepointPath>

# 取消应用
./bin/flink cancel <jobId>
./bin/flink cancel -s [savepointPath] <jobId> # 取消前生成保存点

# 从保存点启动应用
# 如删除，-n 禁用安全检查
./bin/flink run -s <savepointPath> [options] <jobJar> [args]

# 应用的扩缩容
# 生成保存点，取消，以新的并行度重启
./bin/flink modify <jobId> -p <newParallelism>
```

### 通过 REST API 管理应用

```bash
# curl REST
curl -X <HTTP-Method> [-d <params>] http://hostname:port/v1/<REST-point>
curl -X GET http://localhost:8081/v1/overview

# 管理和监控 Flink 集群
GET /overview                    # 集群基本信息
GET /jobmanager/config           # JobManager 配置
GET /taskmanagers                # 列出所有相连的 TaskManager
GET /jobmanager/metrics          # 列出 JobManager 可用指标
GET /taskmanagers/<tmId>/metrics # 列出 TaskManager 收集指标
DELETE /cluster                  # 关闭集群

# 管理和监控 Flink 应用
POST /jars/upload/               # 上传 JAR包
GET /jars                        # 列出所有已上传 JAR包
DELETE /jars/<jarId>             # 删除 JAR包
POST /jars/<jarId>/run           # 启动应用
GET /jobs                        # 列出应用
GET /jobs/<jobId>                # 展示应用详细信息
PATCH /jobs/<jobId>              # 取消应用
POST /jobs/<jobId>/saveopints    # 生成保存点
POST /savepoint-disposal         # 删除保存点
PATCH /jobs/<jobId>/rescaling    # 应用扩缩容
```

### 在容器中打包并部署应用

- **构建Docker镜像**

```bash
# 针对特定作业构建 Flink Docker 镜像
./flink-container/docker/build.sh \
  -- from-archive <path-to-Flink-1.7.1-archive> \
  -- job-jar <path-to-example-apps-JAR-file> \
  -- image-name flink-book-apps

# 部署在 Docker 的 1个主容器+3个工作容器 上
FLINK_DOCKER_IMAGE_NAME=flink-book-jobs \
  FLINK-JOB=io.github.streamingwithflink.chapter1.AverageSensorReadings \
  DEFAULT_PARALLELISM=3 \
  docker-compose up -d
```
- **在Kurbernetes上运行镜像**

```bash
# ./flink-container/kurbernetes
kubectl create -f job-cluster-service.yaml
kubectl create -f job-cluster-job.yaml
kubectl create -f task-manager-deployment.yaml
```
## 控制任务调度

* **控制任务链接**
  * 完全禁用：StreamExecutionEnvironment.disableOperatorChaining()
  * 禁用算子：input.filter(...).map(new Map1()).map(new Map2()).disableChaining().filter(...)
  * 开启新的链接：input.map(new Map1()).map(new Map2()).startNewChain().filter(...)
    * disableChaining 不能与前后链接，startNewChain 可以与后续链接
* **定义 slot 共享组**
  * slotSharingGroup(String)
  * 如果算子所有输入都属于统一共享组则继承，否则加入 default 组

## 调整检查点及恢复

### 配置检查点

```scala
val env = StreamExecutionEnvironment = ...
env.enableCheckpointing(10000) // 10sec

// 配置检查点
val cpConcfig: CheckpointConfig = env.getCheckpointConfig
cpConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE) // 至少一次
cpConfig.setMinPauseBetweenCheckpoints(30000) // 至少不受干扰处理 30sec
cpConfig.setMaxConcurrentCheckpoints(3) // 允许同时生成三个检查点（默认不允许）
cpConfig.setCheckpointTimeout(30000) // 检查点生成必须在 5min 内完成，否则终止执行
cpConfig.setFailOnCheckpointingErrors(false) // 不因检查点生成错误导致作业失败

// 支持检查点压缩
env.getConfig.setUseSnapshotCompression(true) // RocksDB 增量检查点不支持

// 应用停止后保留检查点
cpConfig.enableExternalizedCheckpoints(
  ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
```

### 配置状态后端

```scala
val env = StreamExecutionEnvironment = ...
val stateBackend: StateBackend = ...
env.setStateBackend(stateBackend)

// 状态后端
val memBackend = new MemoryStateBackend() // 默认异步，限制状态 5MB
val fsBackend = new FsStateBackend("file:///tmp/ckp", true) // 默认开启异步
val rocksBackend = new RocksDBStateBackend("file:///tmp/ckp", true) // 增量，总是异步

// 配置选项
val backend: ConfigurableStateBakcend = ...
val sbConfig = new Configuration()
sbConfig.setBoolean("state.backend.async", true)
sbConfig.setString("state.savepoints.dir", "file:///tmp/svp")
val configuredBackend = backend.configure(sbConfig)

// RocksDB 参数调整
val backend: RocksDBStateBackend = ...
backend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED) // 机械硬盘
```

### 配置故障恢复

* **重启策略**
  * fixed-delay：固定间隔尝试重启某个固定次数
  * failure-rate：允许在未超过故障率的前提下不断重启（e.g. 过去十分钟内不超过三次）
  * no-restart：不会重启，立即失败

```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setRestartStrategy(
  RestartStrategies.fixedDelayRestart(
    5,                             // 重启尝试次数，默认 Int.MAX_VALUE
    Time.of(30, TimeUnit.SECONDS)  // 尝试之间的延迟，默认 10sec
))
```

* **本地恢复**
  * 提高恢复速度，远程系统作为参照的真实数据（the source of truth）
  * flink-conf.yaml
    * state.backend.local-recovery：默认禁用
    * taskmanager.state.local.root-dirs：一个/多个本地路径

## 监控 Flink 集群和应用

* **Flink Web UI**：http://\<jobmanager-hostname>:8081
* **指标系统**
  * 注册和使用指标：MetricGroup（RuntimeContext.getMetrics()）
  * 指标组：Counter（计数）、Gauge（计算值）、Histogram（直方图）、Meter（速率）
  * 域和格式指标：系统域.\[用户域].指定名称（localhost.taskmanager.512.MyMetrics.myCounter）
    * counter = getRuntimeContext.getMetricGroup.addGroup("MyMetrics").counter("myCounter")
  * 发布指标：汇报器（reporter）

```scala
class PositiveFilter extends RichFilterFunction[Int] {

  @transient private var counter: Counter = _
  override def open(parameter: Configuration): Unit = {
    counter = getRuntimeContext.getMetricGroup.counter("droppedElements")
  }

  override def filter(value: Int): Boolean = {
    if (value > 0) {
      true
    } else {
      counter.inc()
      false
    }
  }
}       
```

* **延迟监控**

```scala
env.getConfig.setLatencyTrackingInterval(500L) // ms
```

## 配置日志行为

* conf/log4j.properties
  * log4j.rootLogger=WARN
* 其他配置
  * JVM：-Dlog4j.configuration=parameter
  * 命令行：log4j-cli.properties
  * 会话：log4j-yarn-session.properties

```scala
import org.apache.flink.api.common.functions.MapFunctions
import org.slf4j.LoggerFactory
import org.slf4j.Logger

class MyMapFunction extends MapFunctions[Int, String] {

  Logger LOG = LoggerFactory.getLogger(MyMapFunctions.class)

  override def map(value: Int): String = {
    LOG.info("Converting value {} to string.", value)
    value.toString
  }
}
```
