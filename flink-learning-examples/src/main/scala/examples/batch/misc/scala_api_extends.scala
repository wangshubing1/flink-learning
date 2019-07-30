package examples.batch.misc

import org.apache.flink.api.scala._

//对DataStream拓展
import org.apache.flink.api.scala.extensions._
//对Dataset拓展
//import org.apache.flink.examples.streaming.api.scala.extensions.acceptPartialFunctions

/**
  * @Author: king
  * @Date: 2019-07-30
  * @Desc: TODO scala api 扩展
  */

object scala_api_extends {
  case class Point(x: Double, y: Double)
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    ds.filterWith {
      case Point(x, _) => x > 1
    }.reduceWith {
      case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    }.mapWith {
      case Point(x, y) => (x, y)
    }.flatMapWith {
      case (x, y) => Seq("x" -> x, "y" -> y)
    }.groupingBy {
      case (id, value) => id
    }
  }

}
