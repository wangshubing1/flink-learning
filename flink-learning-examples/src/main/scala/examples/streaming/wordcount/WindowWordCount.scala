package examples.streaming.wordcount

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author: king
  * @Date: 2019-07-26
  * @Desc: TODO 
  */

object WindowWordCount {
  /**
    * 要运行示例程序，首先从终端使用netcat启动输入流：
    * nc -lk 9999
    *
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.print()

    env.execute("Window Stream WordCount")
  }

}
