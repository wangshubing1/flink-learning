# Flink并发运行
一个Flink程序可以由不同的task(如:transformations/opterators,data sources及data sinks等)组成，
一个task会分发到多个并发实例中运行，并且每个并发实例处理task的部分输入数据集。
一个task的并发实例数叫做parallelism。
## 设置Parallelism
task的parallelism可以在Flink的不同级别上指定:
### 算子(operator)级别
每个operator、data source或者data sink都可以通过调用setParallelism()方法来指定parallelism，例如:
* Java代码:


```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> text = [...]
DataStream<Tuple2<String, Integer>> wordCounts = text
    .flatMap(new LineSplitter())
    .keyBy(0)
    .timeWindow(Time.seconds(5))
    .sum(1).setParallelism(5);

wordCounts.print();

env.execute("Word Count Example");
```

* Scala代码

```scala
val env =StreamExecutionEnvironment.getExecutionEnvironment

val text = [...]
val wordCounts = text
    .flatMap{ _.split(" ") map { (_, 1) } }
    .keyBy(0)
    .timeWindow(Time.seconds(5))
    .sum(1).setParallelism(5)
wordCounts.print()

env.execute("Word Count Example")
```

### 运行环境级别
正如此处所提醒的一样,Flink程序是在一个运行环境的上下文中运行的。一个运行环境为每个operator、data source和data sink的运行定义了一个默认的并发数。运行环境的并发数可以被每个算子确切的并发数配置所覆盖。
运行环境的默认并发数可以通过调用setParallelism()方法来指定。为了让所有的operator、data source和data sink以3个并发数来运行，你可按如下方法来设置运行环境的默认并发数:
* Java代码:


```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(3);

DataStream<String> text = [...]
DataStream<Tuple2<String, Integer>> wordCounts = [...]
wordCounts.print();

env.execute("Word Count Example");
```

* Scala代码:


```
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setParallelism(3)

val text = [...]
val wordCounts = text
    .flatMap{ _.split(" ") map { (_, 1) } }
    .keyBy(0)
    .timeWindow(Time.seconds(5))
    .sum(1)
wordCounts.print()

env.execute("Word Count Example")
```

### 客户端级别
并发数可以在提交Job到Flink的客户端设置，客户端可以是Java或者Scala程序，典型的例子如:Flink的命令行接口(CLI).
对于CLI客户端，并发参数可以通过-p来指定，例如:
./bin/flink run -p 10 ../examples/*WordCount-java*.jar

在Java/Scala程序中，可以按如下方式指定:
* Java代码:

```java
try {
    PackagedProgram program = new PackagedProgram(file, args)
    InetSocketAddress jobManagerAddress = RemoteExecutor.getInetFromHostport("localhost:6123")
    Configuration config = new Configuration()

    Client client = new Client(jobManagerAddress, new Configuration(), program.getUserCodeClassLoader())

    // 将并发度设为10
    client.run(program, 10, true)

} catch {
    case e: Exception => e.printStackTrace
}

```
* Scala代码:

```
try {
    val program = new PackagedProgram(file, args)
    val jobManagerAddress = RemoteExecutor.getInetFromHostport("localhost:6123")
    val config = new Configuration()

    val client = new Client(jobManagerAddress, config, program.getUserCodeClassLoader())

    // 将并发度设为10
    client.run(program, 10, true)

} catch {
    case e: Exception => e.printStackTrace
}

```
### 系统级别
影响所有运行环境的系统级别的默认并发度可以在./conf/flink-conf.yaml的parallelism.defaul项中指定，更多详情请参考这里
### 设置最大并发度
最大并发度可以在你设置并发度的地方设置(除开客户端级别和系统级别外)，你可以通过调用setMaxParallelism()方法来设置最大并发度。
默认的最大并发度大致为operatorParallelism + (parallelism/2) ，其中下限为127，上限为32768.


