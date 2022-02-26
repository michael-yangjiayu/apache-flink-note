# 8. 读写外部系统

## 应用的一致性保障

* **数据源连接器**
  * 基于文件的连接器（文件字节流的读取偏移）
  * Kafka连接器（消费topic分区的读取偏移）
* **数据汇连接器**
  * 幂等性写：故障恢复过程中，临时性不一致（e.g. 将相同的键值对插入HashMap）
  * 事务性写：只有在上次成功的检查点之前计算的结果才被写入（延迟）
  * WAL 数据汇（write-ahead log）：将所有结果记录写入应用状态，并在检查点完成后发送
  * 2PC 数据汇（two-phase commit）：收到检查点完成通知后，提交事务真正写入结果

|         | 不可重置数据源 | 可重置数据源 |
| ------- | ------- | ------ |
| 任意数据汇   | 至多一次    | 至少一次   |
| 幂等性数据汇  | 至多一次    | 精确一次   |
| WAL 数据汇 | 至多一次    | 至少一次   |
| 2PC 数据汇 | 至多一次    | 精确一次   |

## 内置连接器

### Kafka 数据源

```scala
// org.apache.flink.flink-connector-kafka_2.12 (v1.7.1)
val properties = new Properties()
properties.setProperty("bootstraip.servers", "localhost:9092")
properties.setProperty("group.id", "test")

val stream: DataStream[String] = env.addSource(
    new FlinkKafkaConsumer[String](
        "topic",                      // topic: 单个/列表/正则表达式
        new SimpleStringSchema(),     // (Keyed)DeserializationSchema
        properties))                  // properties

FlinkKafkaConsumer.assignTimestampAndWatermark()        // 时间戳、水位线
    - AssignerWithPeriodicWatermark
    - AssignerWithPunctuatedWatermark

FlinkKafkaConsumer.setStartFromGroupoffsets()           // 最后读取位置（group.id）
FlinkKafkaConsumer.setStartFromEarliest()               // 每个分区最早的偏移
FlinkKafkaConsumer.setStartFromLatest()                 // 每个分区最晚的偏移
FlinkKafkaConsumer.setStartFromTimestamp(long)          // 时间戳大于给定值的记录
FlinkKafkaConsumer.setStartFromSpeecificOffsets(Map)    // 为所有分区指定读取位置

// 自动发现满足正则表达式的新主题
// properties.setProperty("flink.partition-discorvery.interval-millis", "1000")
```

### Kafka 数据汇

```scala
// org.apache.flink.flink-connector-kafka_2.12 (v1.7.1)
val stream: DataStream[String] = ...
val myProducer = new FlinkKafkaProducer[String](
    "localhost:9092",        // Kafka Broker 列表 (,)
    "topic",                 // 目标 topic
    new SimpleStringSchema)  // 序列化的 Schema
stream.addSink(myProducer)
```

* **至少一次保障**
  * Flink检查点开启，且所有数据源可重置
  * 如果数据汇连接器写入不成功，则会抛出异常（default）
  * 数据汇连接器要在检查点完成前等待Kafka确认写入完毕（default）
* **精确一次保障**（v0.11）
  * Semantic.NONE
  * Semantic.AT\_LEAST\_ONCE (default)
  * Semantic.EXACTLY\_ONCE：全部消息追加到分区日志， 并在事务提交后标记为已提交（延迟）
* **自定义分区和写入消息时间戳**
  * 自定义分区：提供 KeyedSerializationSchema，FlinkKafkaPartitioner=null 禁用默认分区器
  * 写入时间戳（v0.10）：setWriteTimestamp ToKafka(true)

### 文件系统数据源

```scala
val lineReader = new TextInputFormat(null)
val lineStream: DataStream[String] = env.readFile[String](
    lineReader,                                  // FileInputFormat
    "hdfs:///path/to/my/data",                   // 读取路径
    FileProcessing.Mode.PROCESS_CONTINUOUSLY,    // 处理模式，或 PROCESS_ONCE
    30000L)                                      // 监控间隔（ms）
```

### 文件系统数据汇

```scala
val input: DataStream[String] = ...
val sink: StreamingFileSink[String] = StreamingFileSink
    .forRowFormat(                                // 行编码
        new Path("/base/path"),
        new SimpleStringEncoder[String]("UTF-8"))
    .build()
input.addSink(sink)
```

* **桶的选择** `BucketAssigner` (default = `DataTimeBucketAssigner`)
  * 路径规则：\[base-path]/\[bucket-part]]/part-\[task-idx]-\[id]
  * RollingPolicy：何时创建一个新的分块文件（默认文件大小超过128M或事件超过60秒）
* **两种分块文件写入模式**
  * 行编码（row encoding）：将每条记录单独写入分块文件中（SimpleStringEncoder）
  * 批量编码（bulk encoding）：记录会被攒成批，然后一次性写入，如果 Apache Parquet

```scala
val input: DataStream[String] = ...
val sink: StreamingFileSink[String] = StreamingFileSink
    .forBulkFormat(                                // 批量编码
        new Path("/base/path"),
        ParquetAvroWriter.forSpecificRecord(classof[AvroPojo]))
    .build()
input.addSink(sink)
```

### Cassandra 数据汇

- **针对元组**

```scala
// org.apache.flink.flink-connector-cassandra_2.12 (v1.7.1)
// 定义 Cassandra 示例表
CREATE KEYSPACE IF NOT EXISTS example
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
CREATE TABLE IF NOT EXISTS example.sensors (
    sensord VARCHAR,
    temperature FLOAT,
    PRIMARY KEY(sensorId)
);

// 针对元组的 Cassandra 数据汇
val readings: DataStream[(String, Float)] = ...
val sinkBuilder: CassandraSinkBuilder[(String, Float)] =
    CassandraSink.addSink(readings)
sinkBuilder.setHost("localhost").setQuery(
    "INSERT INTO example.sensor(sensorId, temperature) VALUES (?, ?);")
    .build()
```
- **针对 POJO 类型**

```scala
// 针对 POJO 类型创建 Cassandra 数据汇
val readings: DataStream[(String, Float)] = ...
CassandraSink.addSink(readings).setHost("localhost").build()

// 添加了 Cassandra Object Mapper 注释的 POJO 类
@Table(keyspace = "example", name = "sensors")
class SensorReading(
    @Column(name = "sensorId") var id: String,
    @Column(name = "temporature) var temp: Float) {
    
    def this() = this("", 0.0)
    def setId(id: String): Unit = this.id = id
    def getid: String = id
    def setTemp(temp: Float): Unit = this.temp = temp
    def getTemp: Float = temp
}
```
* setClusterBuilder(ClusterBuilder)：管理和 Cassandra 的连接
* setHost(String, \[Int])
* setQuery(String)
* setMapperOptions(MapperOptions)
* enableWriteAheadLog(\[CheckpointCommiter])：开启 WAL，提供精确一次输出保障

## 实现自定义数据源函数

* SourceFunction 和 RichSourceFunction：定义非并行的数据源连接器（单任务运行）
* ParallelSourceFunction 和 ParallelRichSourceFunction：定义并行的数据源连接起
* (Parallel)SourceFunction：run() 执行记录读入，cancel() 在取消或关闭时调用
* **可重置的数据源函数**
  * 实现 CheckpointedFunction 接口，并把读取偏移和相关元数据信息存入状态
  * run() 不会再检查点生成过程中推进偏移/发出数据：SourceContext.getCheckpointLock()
* **数据源函数、时间戳及水位线**
  * SourceContext
    * collectWithTimestamp(record: T, timestamp: Long): Unit
    * def emitWatermark(watermark: Watermark): Unit
  * 标记为暂时空闲：SourceContext.markAsTemporarilyIdle()

- **SourceFunction**

```scala
// 从 0 数到 LongMaxValue 的 SourceFunction
class CountSource extends SourceFunction[Long] {
  var isRunning: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    var cnt: Long = -1
    while (isRunning && cnt < Long.MaxValue) {
      // increment cnt
      cnt += 1
      ctx.collect(cnt)
    }
  }

  override def cancel(): Unit = isRunning = false
}
```
- **可重置的 SourceFunction**

```scala
// 可重置的 SourceFunction
class ReplayableCountSource
    extends SourceFunction[Long] with CheckpointedFunction {

  var isRunning: Boolean = true
  var cnt: Long = _
  var offsetState: ListState[Long] = _

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning && cnt < Long.MaxValue) {
      ctx.getCheckpointLock.synchronized {
        // increment cnt
        cnt += 1
        ctx.collect(cnt)
      }
    }
  }

  override def cancel(): Unit = isRunning = false

  override def snapshotState(snapshotCtx: FunctionSnapshotContext): Unit = {
    // remove previous cnt
    offsetState.clear()
    // add current cnt
    offsetState.add(cnt)
  }

  override def initializeState(initCtx: FunctionInitializationContext): Unit = {
    // obtain operator list state to store the current cnt
    val desc = new ListStateDescriptor[Long]("offset", classOf[Long])
    offsetState = initCtx.getOperatorStateStore.getListState(desc)
    // initialize cnt variable from the checkpoint
    val it = offsetState.get()
    cnt = if (null == it || !it.iterator().hasNext) {
      -1L
    } else {
      it.iterator().next()
    }
  }
}
```
## 实现自定义数据汇函数

* SinkFunction.invoke(value: IN, ctx: Context)：e.g. 套接字
* 幂等性数据汇连接器：e.g. Derby
* 事务性数据汇连接器：实现 CheckpointListener 接口
  * GennericWriteAheadSink：至少一次
    * 参数：CheckopintCommiter, TypeSerializer, 任务id
    * 方法：boolean sendValues(Iterable\<IN> values, long chkpntId, long timestamp)
  * TwoPhaseCommitSinkFunction：精确一次
    * 要求
      * 外部数据汇支持事务、检查点间隔期事务开启、事务接到完成通知后提交
      * 数据汇需要在进程故障时进行事务恢复、提交事务的操作是幂等的
    * 方法：
      * beginTransaction()
      * invoke(txn: TXN, value: IN, context: Context\[\_])
      * preCommit(txn: TXN)、commit(txn: TXN)
      * abort(txn: TXN)

- **SinkFunction**

```scala
// nc -l localhost 9091
// write the sensor readings to a socket
val reading: DataStream[SensorReading] = ...
readings.addSink(new SimpleSocketSink("localhost", 9191).setParallelism(1)

// Writes a stream of [[SensorReading]] to a socket.
class SimpleSocketSink(val host: String, val port: Int)
    extends RichSinkFunction[SensorReading] {

  var socket: Socket = _
  var writer: PrintStream = _

  override def open(config: Configuration): Unit = {
    // open socket and writer
    socket = new Socket(InetAddress.getByName(host), port)
    writer = new PrintStream(socket.getOutputStream)
  }

  override def invoke(
      value: SensorReading,
      ctx: SinkFunction.Context[_]): Unit = {
    // write sensor reading to socket
    writer.println(value.toString)
    writer.flush()
  }

  override def close(): Unit = {
    // close writer and socket
    writer.close()
    socket.close()
  }
}
```
- **IdempotentSinkFunction**

```scala
// write the converted sensor readings to Derby.
val reading: DataStream[SensorReading] = ...
readings.addSink(new DerbyUpsertSink)

// Sink that upserts SensorReadings into a Derby table
class DerbyUpsertSink extends RichSinkFunction[SensorReading] {

  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    // connect to embedded in-memory Derby
    val props = new Properties()
    conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", props)
    // prepare insert and update statements
    insertStmt = conn.prepareStatement(
      "INSERT INTO Temperatures (sensor, temp) VALUES (?, ?)")
    updateStmt = conn.prepareStatement(
      "UPDATE Temperatures SET temp = ? WHERE sensor = ?")
  }

  override def invoke(r: SensorReading, context: Context[_]): Unit = {
    // set parameters for update statement and execute it
    updateStmt.setDouble(1, r.temperature)
    updateStmt.setString(2, r.id)
    updateStmt.execute()
    // execute insert statement if update statement did not update any row
    if (updateStmt.getUpdateCount == 0) {
      // set parameters for insert statement
      insertStmt.setString(1, r.id)
      insertStmt.setDouble(2, r.temperature)
      // execute insert statement
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
```
- **WAL**

```scala
// print to standard out with a write-ahead log.
// results are printed when a checkpoint is completed.
val readings: DataStream[SensorReading] = ...
readings.transform("WriteAheadSink", new StdOutWriteAheadSink).setParallelism(1)

// Write-ahead sink that prints to standard out and
// commits checkpoints to the local file system.
class StdOutWriteAheadSink extends GenericWriteAheadSink[(String, Double)](
    // CheckpointCommitter that commits checkpoints to the local file system
    new FileCheckpointCommitter(System.getProperty("java.io.tmpdir")),
    // Serializer for records
    createTypeInformation[(String, Double)].createSerializer(new ExecutionConfig),
    // Random JobID used by the CheckpointCommitter
    UUID.randomUUID.toString) {

  override def sendValues(
      readings: Iterable[(String, Double)],
      checkpointId: Long,
      timestamp: Long): Boolean = {

    for (r <- readings.asScala) {
      // write record to standard out
      println(r)
    }
    true
  }
}
```
- **2PC**

```scala
class TransactionalFileSink(val targetPath: String, val tempPath: String)
    extends TwoPhaseCommitSinkFunction[(String, Double), String, Void](
      createTypeInformation[String].createSerializer(new ExecutionConfig),
      createTypeInformation[Void].createSerializer(new ExecutionConfig)) {

  var transactionWriter: BufferedWriter = _

  /** Creates a temporary file for a transaction into which the records are
    * written.
    */
  override def beginTransaction(): String = {

    // path of transaction file is constructed from current time and task index
    val timeNow = LocalDateTime.now(ZoneId.of("UTC"))
      .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask
    val transactionFile = s"$timeNow-$taskIdx"

    // create transaction file and writer
    val tFilePath = Paths.get(s"$tempPath/$transactionFile")
    Files.createFile(tFilePath)
    this.transactionWriter = Files.newBufferedWriter(tFilePath)
    println(s"Creating Transaction File: $tFilePath")

    // name of transaction file is returned to later identify the transaction
    transactionFile
  }

  /** Write record into the current transaction file. */
  override def invoke(transaction: String, value: (String, Double),
    context: Context[_]): Unit = {

    transactionWriter.write(value.toString)
    transactionWriter.write('\n')
  }

  /** Flush and close the current transaction file. */
  override def preCommit(transaction: String): Unit = {
    transactionWriter.flush()
    transactionWriter.close()
  }

  /** Commit a transaction by moving the pre-committed transaction file
    * to the target directory.
    */
  override def commit(transaction: String): Unit = {
    val tFilePath = Paths.get(s"$tempPath/$transaction")
    // check if the file exists to ensure that the commit is idempotent.
    if (Files.exists(tFilePath)) {
      val cFilePath = Paths.get(s"$targetPath/$transaction")
      Files.move(tFilePath, cFilePath)
    }
  }

  /** Aborts a transaction by deleting the transaction file. */
  override def abort(transaction: String): Unit = {
    val tFilePath = Paths.get(s"$tempPath/$transaction")
    if (Files.exists(tFilePath)) {
      Files.delete(tFilePath)
    }
  }
}
```
## 异步访问外部系统

* AsyncFunction
  * orderedWait()：按照数据记录顺序发出结果的异步算子
  * unorderedWait()：只能让水位线和检查点分隔符保持对齐
* 示例中为每条记录创建 JDBC 连接，实际应用中需要避免

- **orderedWait**

```scala
// look up the location of a sensor from a Derby table with asynchronous requests.
val readings: DataStream[SensorReading] = ...
val sensorLocations: DataStream[(String, String)] = AsyncDataStream
  .orderedWait(
    readings,
    new DerbyAsyncFunction,
    5, TimeUnit.SECONDS,        // timeout requests after 5 seconds
    100)                        // at most 100 concurrent requests
```
- **DerbyAsyncFunction**

```scala
// AsyncFunction that queries a Derby table via JDBC in a non-blocking fashion.
class DerbyAsyncFunction extends AsyncFunction[SensorReading, (String, String)] {

  // caching execution context used to handle the query threads
  private lazy val cachingPoolExecCtx =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  // direct execution context to forward result future to callback object
  private lazy val directExecCtx =
    ExecutionContext.fromExecutor(
      org.apache.flink.runtime.concurrent.Executors.directExecutor())

  /** Executes JDBC query in a thread and handles the resulting Future
    * with an asynchronous callback. */
  override def asyncInvoke(
      reading: SensorReading,
      resultFuture: ResultFuture[(String, String)]): Unit = {

    val sensor = reading.id

    // get room from Derby table as Future
    val room: Future[String] = Future {
      // Creating a new connection and statement for each record.
      // Note: This is NOT best practice!
      // Connections and prepared statements should be cached.
      val conn = DriverManager
        .getConnection("jdbc:derby:memory:flinkExample", new Properties())
      val query = conn.createStatement()

      // submit query and wait for result. this is a synchronous call.
      val result = query.executeQuery(
        s"SELECT room FROM SensorLocations WHERE sensor = '$sensor'")

      // get room if there is one
      val room = if (result.next()) {
        result.getString(1)
      } else {
        "UNKNOWN ROOM"
      }

      // close resultset, statement, and connection
      result.close()
      query.close()
      conn.close()

      // sleep to simulate (very) slow requests
      Thread.sleep(2000L)

      // return room
      room
    }(cachingPoolExecCtx)

    // apply result handling callback on the room future
    room.onComplete {
      case Success(r) => resultFuture.complete(Seq((sensor, r)))
      case Failure(e) => resultFuture.completeExceptionally(e)
    }(directExecCtx)
  }
}
```
