package examples.streaming.File

import java.io.{BufferedInputStream, InputStream}
import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author: king
  * @Date: 2019-07-31
  * @Desc: TODO 
  */

object FlinkReadFileStream {
  def main(args: Array[String]): Unit = {
    val filePath1 = initBaseProp.getProperty("file1")
    val filePath2 = initBaseProp.getProperty("file2")
    val windowTime = initBaseProp.getProperty("windowTime")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val unionStream = if (filePath1 != null && filePath2 != null) {
      val stream1 = env.readTextFile(filePath1)
      val stream2 = env.readTextFile(filePath2)
      stream1.union(stream2)
    } else {
      println("Error: filePath maybe null")
      null
    }
    unionStream.map(x => getRecord(x))
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[UserRecord] {
        override def checkAndGetNextWatermark(lastElement: UserRecord, extractedTimestamp: Long): Watermark = {
          new Watermark(System.currentTimeMillis() - 60)
        }

        override def extractTimestamp(element: UserRecord, previousElementTimestamp: Long): Long = {
          System.currentTimeMillis()
        }
      })
      .filter(_.name != "GuoYijun")
      //分组  keyBy(0) 指定整个tuple作为key
      .keyBy("name", "sexy")
      //.keyBy("shoppingTime")
      //窗口 使用滚动窗口,滑动窗口
      .window(TumblingEventTimeWindows.of(Time.seconds(windowTime.toInt)))
      //聚合 reduce
      .reduce((e1, e2) => UserRecord(e1.name, e1.sexy, e1.shoppingTime + e2.shoppingTime))
      .print()

    env.execute("FlinkReadFileStream")
  }

  def initBaseProp: Properties = {
    val prop: Properties = new Properties
    val in: InputStream = getClass.getResourceAsStream("/base.properties")
    if (in == null) {
      println("ERROR : base's properties init failed in is null")
    }
    prop.load(new BufferedInputStream(in))
    prop
  }

  def getRecord(line: String): UserRecord = {
    val elems = line.split(",")
    assert(elems.length == 3)
    val name = elems(0)
    val sexy = elems(1)
    val time = elems(2).toInt
    UserRecord(name, sexy, time)
  }

  //内置pojo类
  case class UserRecord(name: String, sexy: String, shoppingTime: Int)

  private class Record2TimestampExtractor extends AssignerWithPunctuatedWatermarks[UserRecord] {
    override def checkAndGetNextWatermark(lastElement: UserRecord, extractedTimestamp: Long):Watermark = {
      new Watermark(extractedTimestamp - 1)
    }

    override def extractTimestamp(element: UserRecord, previousElementTimestamp: Long):Long = {
      System.currentTimeMillis()
    }

}

}
