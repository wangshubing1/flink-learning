package batch

import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool


/**
  * @Author: king
  * @Date: 2019-07-24
  * @Desc: TODO
  */

object WordCount {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    //获取输入
    val text =
      if (params.has("input")) {
        env.readTextFile(params.get("input"))
      } else {
        println("Executing WordCount example with default input data set.")
        println("Use --input to specify file input.")
        env.fromCollection(WordCountData.WORDS)
      }
    //核心代码
    val counts = text.flatMap {_.toLowerCase.split("\\W+") filter {_.nonEmpty}}
      .map {(_, 1)}
      .groupBy(0)
      .sum(1)
    if (params.has("output")) {
      counts.writeAsCsv(params.get("output"), "\n", " ")
      env.execute("Scala WordCount Example")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }
  }

}
