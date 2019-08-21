# Flink 异步通信
## 1 概述

流计算系统中经常需要与外部系统进行交互，我们通常的做法如向数据库发送用户a的查询请求，然后等待结果返回，在这之前，我们的程序无法发送用户b的查询请求。这是一种同步访问方式，如下图所示。
![](https://ci.apache.org/projects/flink/flink-docs-release-1.8/fig/async_io.svg)
与数据库的异步交互意味着单个并行函数实例可以同时处理许多请求并同时接收响应。这样，等待时间可以覆盖发送其他请求和接收响应。至少，等待时间是在多个请求上摊销的。这导致大多数情况下流量吞吐量更高。
## 2 异步I / O API
Flink的Async I / O API允许用户将异步请求客户端与数据流一起使用。API处理与数据流的集成，以及处理顺序，事件时间，容错等。

假设有一个目标数据库的异步客户端，需要三个部分来实现对数据库的异步I / O流转换：

* 实现AsyncFunction是把请求分派
* 一个回调，它接受操作的结果并将其交给ResultFuture
* 在DataStream上应用异步I / O操作作为转换
Java 代码

```java

// This example implements the asynchronous request and callback with Futures that have the
// interface of Java 8's futures (which is the same one followed by Flink's Future)

/**
 * An implementation of the 'AsyncFunction' that sends requests and sets the callback.
 */
class AsyncDatabaseRequest extends RichAsyncFunction<String, Tuple2<String, String>> {

    /** The database specific client that can issue concurrent requests with callbacks */
    private transient DatabaseClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        client = new DatabaseClient(host, post, credentials);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public void asyncInvoke(String key, final ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {

        // issue the asynchronous request, receive a future for result
        final Future<String> result = client.query(key);

        // set the callback to be executed once the request by the client is complete
        // the callback simply forwards the result to the result future
        CompletableFuture.supplyAsync(new Supplier<String>() {

            @Override
            public String get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // Normally handled explicitly.
                    return null;
                }
            }
        }).thenAccept( (String dbResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<>(key, dbResult)));
        });
    }
}

// create the original stream
DataStream<String> stream = ...;

// apply the async I/O transformation
DataStream<Tuple2<String, String>> resultStream =
    AsyncDataStream.unorderedWait(stream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100);
```
本实例展示了flink Async I/O的基本用法，
* 首先是实现AsyncFunction接口，用于编写异步请求逻辑及将结果或异常设置到resultFuture
* 然后就是使用AsyncDataStream的unorderedWait或orderedWait方法将AsyncFunction作用到DataStream作为transformation；AsyncDataStream的unorderedWait或orderedWait有两个关于async operation的参数，一个是timeout参数用于设置async的超时时间，一个是capacity参数用于指定同一时刻最大允许多少个(并发)async request在执行

Scala 代码

```scala
/**
 * An implementation of the 'AsyncFunction' that sends requests and sets the callback.
 */
class AsyncDatabaseRequest extends AsyncFunction[String, (String, String)] {

    /** The database specific client that can issue concurrent requests with callbacks */
    lazy val client: DatabaseClient = new DatabaseClient(host, post, credentials)

    /** The context used for the future callbacks */
    implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())


    override def asyncInvoke(str: String, resultFuture: ResultFuture[(String, String)]): Unit = {

        // issue the asynchronous request, receive a future for the result
        val resultFutureRequested: Future[String] = client.query(str)

        // set the callback to be executed once the request by the client is complete
        // the callback simply forwards the result to the result future
        resultFutureRequested.onSuccess {
            case result: String => resultFuture.complete(Iterable((str, result)))
        }
    }
}

// create the original stream
val stream: DataStream[String] = ...

// apply the async I/O transformation
val resultStream: DataStream[(String, String)] =
    AsyncDataStream.unorderedWait(stream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100)
```
**重要提示**：ResultFuture在第一次通话时完成ResultFuture.complete。所有后续complete调用都将被忽略。

以下两个参数控制异步操作​​：

* **超时：**超时定义异步请求在被视为失败之前可能需要多长时间。此参数可防止死/失败请求。

* **容量：**此参数定义可以同时进行的异步请求数。尽管异步I / O方法通常会带来更好的吞吐量，但运营商仍然可能成为流应用程序的瓶颈。限制并发请求的数量可确保操作员不会累积不断增长的待处理请求积压，但一旦容量耗尽，它将触发反压。
## 超时处理
当异步I / O请求超时时，默认情况下会引发异常并重新启动作业。如果要处理超时，可以覆盖该AsyncFunction#timeout方法
## 结果顺序
由AsyncFunction一些未定义的顺序经常完成的并发请求，基于哪个请求首先完成。为了控制发出结果记录的顺序，Flink提供了两种模式：

* 无序：异步请求完成后立即发出结果记录。在异步I / O运算符之后，流中记录的顺序与以前不同。当使用处理时间作为基本时间特性时，此模式具有最低延迟和最低开销。使用AsyncDataStream.unorderedWait(...)此模式。

* 有序：在这种情况下，保留流顺序。结果记录的发出顺序与触发异步请求的顺序相同（运算符输入记录的顺序）。为此，运算符缓冲结果记录，直到其所有先前记录被发出（或超时）。这通常会在检查点中引入一些额外的延迟和一些开销，因为与无序模式相比，记录或结果在检查点状态下保持更长的时间。使用AsyncDataStream.orderedWait(...)此模式。


## 源码阅读
### AsyncFunction
flink-examples.streaming-java_2.11-1.8.0-sources.jar!/org/apache/flink/examples.streaming/api/functions/async/AsyncFunction.java


```java
//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.flink.examples.streaming.api.functions.async;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;

@PublicEvolving
public interface AsyncFunction<IN, OUT> extends Function, Serializable {
    void asyncInvoke(IN var1, ResultFuture<OUT> var2) throws Exception;

    default void timeout(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        resultFuture.completeExceptionally(new TimeoutException("Async function call has timed out."));
    }
}

```
> AsyncFunction接口继承了Function，它定义了asyncInvoke方法以及一个default的timeout方法；asyncInvoke方法执行异步逻辑，然后通过ResultFuture.complete将结果设置到ResultFuture，如果异常则通过ResultFuture.completeExceptionally(Throwable)来传递到ResultFuture

### RichAsyncFunction
flink-examples.streaming-java_2.11-1.8.0-sources.jar!/org/apache/flink/examples.streaming/api/functions/async/RichAsyncFunction.java

```java
@PublicEvolving
public abstract class RichAsyncFunction<IN, OUT> extends AbstractRichFunction implements AsyncFunction<IN, OUT> {

	private static final long serialVersionUID = 3858030061138121840L;

	@Override
	public void setRuntimeContext(RuntimeContext runtimeContext) {
		Preconditions.checkNotNull(runtimeContext);

		if (runtimeContext instanceof IterationRuntimeContext) {
			super.setRuntimeContext(
				new RichAsyncFunctionIterationRuntimeContext(
					(IterationRuntimeContext) runtimeContext));
		} else {
			super.setRuntimeContext(new RichAsyncFunctionRuntimeContext(runtimeContext));
		}
	}

	@Override
	public abstract void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception;
```
> RichAsyncFunction继承了AbstractRichFunction，同时声明实现AsyncFunction接口，它不没有实现asyncInvoke，交由子类实现；它覆盖了setRuntimeContext方法，这里使用RichAsyncFunctionRuntimeContext或者RichAsyncFunctionIterationRuntimeContext进行包装
### RichAsyncFunctionRuntimeContext
flink-examples.streaming-java_2.11-1.8.0-sources.jar!/org/apache/flink/examples.streaming/api/functions/async/RichAsyncFunction.java

```java
	private static class RichAsyncFunctionRuntimeContext implements RuntimeContext {
		private final RuntimeContext runtimeContext;

		RichAsyncFunctionRuntimeContext(RuntimeContext context) {
			runtimeContext = Preconditions.checkNotNull(context);
		}

		@Override
		public String getTaskName() {
			return runtimeContext.getTaskName();
		}

		@Override
		public MetricGroup getMetricGroup() {
			return runtimeContext.getMetricGroup();
		}

		@Override
		public int getNumberOfParallelSubtasks() {
			return runtimeContext.getNumberOfParallelSubtasks();
		}

		@Override
		public int getMaxNumberOfParallelSubtasks() {
			return runtimeContext.getMaxNumberOfParallelSubtasks();
		}

		@Override
		public int getIndexOfThisSubtask() {
			return runtimeContext.getIndexOfThisSubtask();
		}

		@Override
		public int getAttemptNumber() {
			return runtimeContext.getAttemptNumber();
		}

		@Override
		public String getTaskNameWithSubtasks() {
			return runtimeContext.getTaskNameWithSubtasks();
		}

		@Override
		public ExecutionConfig getExecutionConfig() {
			return runtimeContext.getExecutionConfig();
		}

		@Override
		public ClassLoader getUserCodeClassLoader() {
			return runtimeContext.getUserCodeClassLoader();
		}

		// -----------------------------------------------------------------------------------
		// Unsupported operations
		// -----------------------------------------------------------------------------------

		@Override
		public DistributedCache getDistributedCache() {
			throw new UnsupportedOperationException("Distributed cache is not supported in rich async functions.");
		}

		@Override
		public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException("State is not supported in rich async functions.");
		}

		@Override
		public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException("State is not supported in rich async functions.");
		}

		@Override
		public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException("State is not supported in rich async functions.");
		}

		@Override
		public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
			throw new UnsupportedOperationException("State is not supported in rich async functions.");
		}

		@Override
		public <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties) {
			throw new UnsupportedOperationException("State is not supported in rich async functions.");
		}

		@Override
		public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
			throw new UnsupportedOperationException("State is not supported in rich async functions.");
		}

		@Override
		public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {
			throw new UnsupportedOperationException("Accumulators are not supported in rich async functions.");
		}

		@Override
		public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
			throw new UnsupportedOperationException("Accumulators are not supported in rich async functions.");
		}

		@Override
		public Map<String, Accumulator<?, ?>> getAllAccumulators() {
			throw new UnsupportedOperationException("Accumulators are not supported in rich async functions.");
		}

		@Override
		public IntCounter getIntCounter(String name) {
			throw new UnsupportedOperationException("Int counters are not supported in rich async functions.");
		}

		@Override
		public LongCounter getLongCounter(String name) {
			throw new UnsupportedOperationException("Long counters are not supported in rich async functions.");
		}

		@Override
		public DoubleCounter getDoubleCounter(String name) {
			throw new UnsupportedOperationException("Long counters are not supported in rich async functions.");
		}

		@Override
		public Histogram getHistogram(String name) {
			throw new UnsupportedOperationException("Histograms are not supported in rich async functions.");
		}

		@Override
		public boolean hasBroadcastVariable(String name) {
			throw new UnsupportedOperationException("Broadcast variables are not supported in rich async functions.");
		}

		@Override
		public <RT> List<RT> getBroadcastVariable(String name) {
			throw new UnsupportedOperationException("Broadcast variables are not supported in rich async functions.");
		}

		@Override
		public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
			throw new UnsupportedOperationException("Broadcast variables are not supported in rich async functions.");
		}
	}

	private static class RichAsyncFunctionIterationRuntimeContext extends RichAsyncFunctionRuntimeContext implements IterationRuntimeContext {

		private final IterationRuntimeContext iterationRuntimeContext;

		RichAsyncFunctionIterationRuntimeContext(IterationRuntimeContext iterationRuntimeContext) {
			super(iterationRuntimeContext);

			this.iterationRuntimeContext = Preconditions.checkNotNull(iterationRuntimeContext);
		}

		@Override
		public int getSuperstepNumber() {
			return iterationRuntimeContext.getSuperstepNumber();
		}

		// -----------------------------------------------------------------------------------
		// Unsupported operations
		// -----------------------------------------------------------------------------------

		@Override
		public <T extends Aggregator<?>> T getIterationAggregator(String name) {
			throw new UnsupportedOperationException("Iteration aggregators are not supported in rich async functions.");
		}

		@Override
		public <T extends Value> T getPreviousIterationAggregate(String name) {
			throw new UnsupportedOperationException("Iteration aggregators are not supported in rich async functions.");
		}
	}
}
```
> RichAsyncFunctionIterationRuntimeContext继承了RichAsyncFunctionRuntimeContext，实现了IterationRuntimeContext接口，它将getSuperstepNumber方法交由IterationRuntimeContext处理，然后覆盖getIterationAggregator、getPreviousIterationAggregate方法抛出UnsupportedOperationException
### AsyncDataStream
flink-examples.streaming-java_2.11-1.8.0-sources.jar!/org/apache/flink/examples.streaming/api/datastream/AsyncDataStream.java

```java

@PublicEvolving
public class AsyncDataStream {

	/**
	 * Output mode for asynchronous operations.
	 */
	public enum OutputMode { ORDERED, UNORDERED }

	private static final int DEFAULT_QUEUE_CAPACITY = 100;


	private static <IN, OUT> SingleOutputStreamOperator<OUT> addOperator(
			DataStream<IN> in,
			AsyncFunction<IN, OUT> func,
			long timeout,
			int bufSize,
			OutputMode mode) {

		TypeInformation<OUT> outTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			func,
			AsyncFunction.class,
			0,
			1,
			new int[]{1, 0},
			in.getType(),
			Utils.getCallLocationName(),
			true);

		// create transform
		AsyncWaitOperator<IN, OUT> operator = new AsyncWaitOperator<>(
			in.getExecutionEnvironment().clean(func),
			timeout,
			bufSize,
			mode);

		return in.transform("async wait operator", outTypeInfo, operator);
	}

	public static <IN, OUT> SingleOutputStreamOperator<OUT> unorderedWait(
			DataStream<IN> in,
			AsyncFunction<IN, OUT> func,
			long timeout,
			TimeUnit timeUnit,
			int capacity) {
		return addOperator(in, func, timeUnit.toMillis(timeout), capacity, OutputMode.UNORDERED);
	}

	public static <IN, OUT> SingleOutputStreamOperator<OUT> unorderedWait(
			DataStream<IN> in,
			AsyncFunction<IN, OUT> func,
			long timeout,
			TimeUnit timeUnit) {
		return addOperator(
			in,
			func,
			timeUnit.toMillis(timeout),
			DEFAULT_QUEUE_CAPACITY,
			OutputMode.UNORDERED);
	}

	public static <IN, OUT> SingleOutputStreamOperator<OUT> orderedWait(
			DataStream<IN> in,
			AsyncFunction<IN, OUT> func,
			long timeout,
			TimeUnit timeUnit,
			int capacity) {
		return addOperator(in, func, timeUnit.toMillis(timeout), capacity, OutputMode.ORDERED);
	}

	public static <IN, OUT> SingleOutputStreamOperator<OUT> orderedWait(
			DataStream<IN> in,
			AsyncFunction<IN, OUT> func,
			long timeout,
			TimeUnit timeUnit) {
		return addOperator(
			in,
			func,
			timeUnit.toMillis(timeout),
			DEFAULT_QUEUE_CAPACITY,
			OutputMode.ORDERED);
	}
}

```
* AsyncDataStream提供了unorderedWait、orderedWait两类方法来将AsyncFunction作用于DataStream
* unorderedWait、orderedWait方法有带capacity参数的也有不带capacity参数的，不带capacity参数即默认使用DEFAULT_QUEUE_CAPACITY，即100；这些方法最后都是调用addOperator私有方法来实现，它使用的是AsyncWaitOperator；unorderedWait、orderedWait方法都带了timeout参数，用于指定等待async操作完成的超时时间
* AsyncDataStream提供了两种OutputMode，其中UNORDERED是无序的，即一旦async操作完成就emit结果，当使用TimeCharacteristic.ProcessingTime的时候这种模式延迟最低、负载最低；ORDERED是有序的，即按element的输入顺序emit结果，为了保证有序operator需要缓冲数据，因而会造成一定的延迟及负载

