package examples.streaming.wordcount

import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @Author: king
  * @Date: 2019-07-26
  * @Desc: TODO 
  */

object SocketTextStreamWordCount {
  def main(args: Array[String]): Unit = {
    //参数检查
    if (args.length != 2) {
      System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>")
    }

    val hostname = args(0)
    val port = args(1).toInt

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val stream: DataStream[String] = env.socketTextStream(hostname, port, '\n')


    val sum = stream.flatMap(new SocketTextStreamWordCount.LineSplitter).keyBy(0).sum(1)
    sum.print

    env.execute("Socket Window WordCount")
  }

  import org.apache.flink.api.common.functions.FlatMapFunction

  final class LineSplitter extends FlatMapFunction[String, (String, Integer)] {
    def flatMap(s: String, collector: Collector[(String, Integer)]): Unit = {
      val tokens = s.toLowerCase.split("\\W+")
      for (token <- tokens) {
        if (token.length > 0) collector.collect(new Tuple2[String, Integer](token, 1))
      }
    }
  }

}
