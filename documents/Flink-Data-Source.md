# DataSource
StreamExecutionEnvironment 中可以使用以下几个已实现的 stream sources
![-w819](http://m.qpic.cn/psb?/V13VPBDG4Ve2nJ/WxJpgxky0dSDzU6prNh8B6yFttshxqRf7l.lv0tBESo!/b/dEEBAAAAAAAA&bo=XAdMAwAAAAADBzY!&rf=viewer_4)
总的来说可以分为下面几大类：
## 基于集合
* fromCollection(Collection) - 从 Java 的 Java.util.Collection 创建数据流。集合中的所有元素类型必须相同。

* fromCollection(Iterator, Class) - 从一个迭代器中创建数据流。Class 指定了该迭代器返回元素的类型。

* fromElements(T …) - 从给定的对象序列中创建数据流。所有对象类型必须相同。

```java
DataStream<Event> input = env.fromElements(
	new Event(1, "barfoo", 1.0),
	new Event(2, "start", 2.0),
	new Event(3, "foobar", 3.0),
	...
);
```
* fromParallelCollection(SplittableIterator, Class) - 从一个迭代器中创建并行数据流。Class 指定了该迭代器返回元素的类型。

* generateSequence(from, to) - 创建一个生成指定区间范围内的数字序列的并行数据流。
## 基于文件
* readTextFile(path) - 读取文本文件，即符合 TextInputFormat 规范的文件，并将其作为字符串返回。


```
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<String> text = env.readTextFile("file:///path/to/file");

```
* readFile(fileInputFormat, path) - 根据指定的文件输入格式读取文件（一次）。

* readFile(fileInputFormat, path, watchType, interval, pathFilter, typeInfo) - 这是上面两个方法内部调用的方法。它根据给定的 fileInputFormat 和读取路径读取文件。根据提供的 watchType，这个 source 可以定期（每隔 interval 毫秒）监测给定路径的新数据（FileProcessingMode.PROCESS_CONTINUOUSLY），或者处理一次路径对应文件的数据并退出（FileProcessingMode.PROCESS_ONCE）。你可以通过 pathFilter 进一步排除掉需要处理的文件。


```
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<MyEvent> stream = env.readFile(
        myFormat, myFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100,
        FilePathFilter.createDefaultFilter(), typeInfo);
```
### 实现:

在具体实现上，Flink 把文件读取过程分为两个子任务，即目录监控和数据读取。每个子任务都由单独的实体实现。目录监控由单个非并行（并行度为1）的任务执行，而数据读取由并行运行的多个任务执行。后者的并行性等于作业的并行性。单个目录监控任务的作用是扫描目录（根据 watchType 定期扫描或仅扫描一次），查找要处理的文件并把文件分割成切分片（splits），然后将这些切分片分配给下游 reader。reader 负责读取数据。每个切分片只能由一个 reader 读取，但一个 reader 可以逐个读取多个切分片。

>重要注意：
* 如果 watchType 设置为 FileProcessingMode.PROCESS_CONTINUOUSLY，则当文件被修改时，其内容将被重新处理。这会打破“exactly-once”语义，因为在文件末尾附加数据将导致其所有内容被重新处理。
* 如果 watchType 设置为 FileProcessingMode.PROCESS_ONCE，则 source 仅扫描路径一次然后退出，而不等待 reader 完成文件内容的读取。当然 reader 会继续阅读，直到读取所有的文件内容。关闭 source 后就不会再有检查点。这可能导致节点故障后的恢复速度较慢，因为该作业将从最后一个检查点恢复读取。

## 基于 Socket：
socketTextStream(String hostname, int port) - 从 socket 读取。元素可以用分隔符切分。

```java
DataStream<Tuple2<String, Integer>> dataStream = env
        .socketTextStream("localhost", 9999) // 监听 localhost 的 9999 端口过来的数据
        .flatMap(new Splitter())
        .keyBy(0)
        .timeWindow(Time.seconds(5))
        .sum(1);
```
## 自定义
addSource - 添加一个新的 source function。例如，你可以 addSource(new FlinkKafkaConsumer011<>(…)) 以从 Apache Kafka 读取数据
