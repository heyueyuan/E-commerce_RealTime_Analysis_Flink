package cn.heyueyuan

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer



case class ServerLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object ServerLogAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.readTextFile("ServerLogAnalysis/src/main/resources/apache.log")
      .map(line => {
        val linearray = line.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(linearray(3)).getTime
        ServerLogEvent(linearray(0), linearray(2), timestamp, linearray(5), linearray(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ServerLogEvent](Time.milliseconds(1000)) {
        override def extractTimestamp(t: ServerLogEvent): Long = {
          t.eventTime
        }
      })
      .filter( data => {
        val pattern = "^((?!\\.(css|js)$).)*$".r
        (pattern findFirstIn data.url).nonEmpty
      })
      .keyBy("url")
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(1)
      .process(new TopNHotUrl(5))
      .print()

    env.execute("sever log")
  }

  class CountAgg() extends AggregateFunction[ServerLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: ServerLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class WindowResultFunction() extends WindowFunction[Long, UrlViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      val url: String = key.asInstanceOf[Tuple1[String]].f0
      val count = input.iterator.next()
      out.collect(UrlViewCount(url, window.getEnd, count))
    }
  }

  class TopNHotUrl(topSize: Int) extends KeyedProcessFunction[Tuple, UrlViewCount, String] {
    private var urlState : ListState[UrlViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      val urlStateDesc = new ListStateDescriptor[UrlViewCount]("urlState-state", classOf[UrlViewCount])
      urlState = getRuntimeContext.getListState(urlStateDesc)
    }

    override def processElement(input: UrlViewCount, context: KeyedProcessFunction[Tuple, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
      urlState.add(input)
      context.timerService().registerEventTimeTimer(input.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allUrlViews : ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (urlView <- urlState.get) {
        allUrlViews += urlView
      }

      val sortedUrlViews = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      val result: StringBuilder = new StringBuilder
      result.append("======================\n")
      result.append("Time: ").append(new Timestamp(timestamp - 1)).append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentUrlView : UrlViewCount = sortedUrlViews(i)
        result.append("No.").append(i+1).append(":")
          .append("Url= ").append(currentUrlView.url)
          .append("Count= ").append(currentUrlView.count).append("\n")
      }
      result.append("======================\n\n")

      Thread.sleep(1000)
      out.collect(result.toString())

    }
  }

}
