package batch.misc

import batch.wordcount.WordCountData
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * @Author: king
  * @Date: 2019-07-24
  * @Desc: TODO 累加器
  */

object Accumulators {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    //val dataSource = env.fromCollection(WordCountData.WORDS)
    val data = env.fromCollection(WordCountData.WORDS)
    val counts = data.flatMap(_.toLowerCase.split("\\W+"))
      .map(new RichMapFunction[String, String] {
        var counter = new IntCounter()

        override def open(parameters: Configuration): Unit = {
          getRuntimeContext.addAccumulator("my-accumulator", counter)
        }

        override def map(in: String): String = {
          counter.add(1)
          in
        }
      }).map((_, 1))
      .groupBy(0)
      .sum(1)

    counts.writeAsText("flink-learning-examples/output/my-accumulator", WriteMode.OVERWRITE).setParallelism(1)
    val res = env.execute("Accumulator Test")
    //获取累加器的结果
    val num = res.getAccumulatorResult[Int]("my-accumulator")
    println(num)

  }

}
