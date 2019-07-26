package streaming.File

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * @Author: king
  * @Date: 2019-07-25
  * @Desc: TODO 从文件读取数据写入到文件
  */

object File2File {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并发
    env.setParallelism(1)

    val data:DataStreamSource[String]= env.readTextFile("/usr/local/Cellar/blink-1.5.1/README.txt")
    data.print()

    //两种格式都行，另外还支持写入到 hdfs
    //data.writeAsText("file:///usr/local/Cellar/blink-1.5.1/README1.txt")
    data.writeAsText("/usr/local/Cellar/blink-1.5.1/README1.txt")
    env.execute()
  }

}
