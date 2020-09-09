package cn.heyueyuan

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, timestamp: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.readTextFile("ServerLogAnalysis/src/main/resources/UserBehavior.csv")
      .map( line=> {
        val data = line.split(",")
        UserBehavior(data(0).toLong, data(1).toLong, data(2).toInt, data(3), data(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .map(x => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(60 * 60))
      .sum(1)
      .print()

    env.execute("page view")
  }

}
