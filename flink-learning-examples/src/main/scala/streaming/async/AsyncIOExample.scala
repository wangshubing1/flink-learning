package streaming.async

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala.async.ResultFuture
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import scala.concurrent.{ExecutionContext, Future}

/**
  * @Author: king
  * @Date: 2019-07-25
  * @Desc: TODO 
  */

object AsyncIOExample {
  def main(args: Array[String]): Unit = {
    val timeout = 10000L
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input = env.addSource(new SimpleSource())
    val asyncMapped =AsyncDataStream.orderedWait(input,timeout,TimeUnit.MILLISECONDS,10){
      (input,collector: ResultFuture[Int])=>Future{
          collector.complete(Seq(input))
      }(ExecutionContext.global)
    }
    asyncMapped.print()
    env.execute("Async I/O job")
  }
}
class SimpleSource extends ParallelSourceFunction[Int]{
    var runing =true
    var counter = 0
    override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
      while (runing){
        sourceContext.getCheckpointLock.synchronized{
          sourceContext.collect(counter)
        }
        counter += 1
        Thread.sleep(10L)
      }
    }

    override def cancel(): Unit = {
      runing =false
    }
  }

