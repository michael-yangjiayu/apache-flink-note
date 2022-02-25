# 4. 设置 Apache Flink 开发环境

## 在 IDE 中导入书中示例

```bash
> git clone https://github.com/streaming-with-flink/examples-scala
```

## 创建 Flink Maven 项目

```bash
mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-quickstart-java \
    -DarchetypeCatalog=https://repository.apache.org/content/repositories/snapshots/ \
    -DarchetypeVersion=1.3-SNAPSHOT \
    -DgroupId=wiki-edits \
    -DartifactId=wiki-edits \
    -Dversion=0.1 \
    -Dpackage=wikiedits \
    -DinteractiveMode=false
```

## 构建 JAR 包

```bash
mvn clean package -Pbuild-jar
```

