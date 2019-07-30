package examples.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Author: king
  * @Date: 2019-07-24
  * @Desc: TODO 
  */

object Test {
  def main(args: Array[String]): Unit = {
    println("hello flink")
    //流处理引擎
    val see =StreamExecutionEnvironment.getExecutionEnvironment
    //批处理引擎
    val env =ExecutionEnvironment.getExecutionEnvironment
  }
}
