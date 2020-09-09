package cn.heyueyuan

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItemsAnaltsis {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputStream : DataStream[String] = env.readTextFile("HotItemsAnalysis/src/main/resources/UserBehavior.csv")
    val dataStream : DataStream[UserBehavior] = inputStream
      .map( data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val aggStream : DataStream[ItemViewCount] = dataStream
        .filter(_.behavior == "pv")
        .keyBy("itemId")
        .timeWindow(Time.hours(1), Time.minutes(5))
        .aggregate(new CountAgg(), new CountViewWindowResult())

    val resultStream : DataStream[String] = aggStream
        .keyBy("windowEnd")
        .process(new TopNHotItems(5))

//    dataStream.print("data")
//    aggStream.print("agg")
    resultStream.print()

    env.execute("hot items")
  }

  class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class CountViewWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId = key.asInstanceOf[Tuple1[Long]].f0
      val windowEnd = window.getEnd
      val count = input.iterator.next()

      out.collect(ItemViewCount(itemId, windowEnd, count))
    }
  }

  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
    private var itemViewCountListState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))
      super.open(parameters)
    }
    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemViewCountListState.add(i)
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allItemViewCounts : ListBuffer[ItemViewCount] = ListBuffer()
      val iter = itemViewCountListState.get().iterator()
      while (iter.hasNext){
        allItemViewCounts += iter.next()
      }

      itemViewCountListState.clear()

      val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      val result : StringBuilder = new StringBuilder
      result.append("WindowEnd Time: ").append(new Timestamp((timestamp - 1))).append("\n")

      for (i <- sortedItemViewCounts.indices) {
        val currentItemViewCount = sortedItemViewCounts(i)
        result.append("No.").append(i + 1).append(": ")
          .append("ItemID = ").append(currentItemViewCount.itemId).append("\t")
          .append("Popularity = ").append(currentItemViewCount.count).append("\n")
      }

      result.append("\n=====================\n\n")

      Thread.sleep(1000)
      out.collect(result.toString())
    }
  }

}
