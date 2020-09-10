package cn.heyueyuan

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UVCount(windowEnd: Long, count: Long)

object UniqueVistor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("ServerLogAnalysis/src/main/resources/UserBehavior.csv")

    val dataStream = inputStream
      .map( line => {
        val data = line.split(",")
        UserBehavior(data(0).toLong, data(1).toLong, data(2).toInt, data(3), data(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.seconds(60 * 60))
      .apply(new UVCountByWindow())
      .print()

    env.execute("Unique Vistor")
  }

  class UVCountByWindow() extends AllWindowFunction[UserBehavior, UVCount, TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UVCount]): Unit = {
      val s: collection.mutable.Set[Long] = collection.mutable.Set()
      var idSet = Set[Long]()

      for (userBehavior <- input) {
        idSet += userBehavior.userId
      }

      out.collect(UVCount(window.getEnd, idSet.size))

    }
  }

}
