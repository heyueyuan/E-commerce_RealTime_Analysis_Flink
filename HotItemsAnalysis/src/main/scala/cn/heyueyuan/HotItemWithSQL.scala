package cn.heyueyuan

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object HotItemsWithSQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("/Users/mark/E-commerce_RealTime_Analysis_Flink/HotItemsAnalysis/src/main/resources/UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val dataTable = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    val aggTable = dataTable
      .filter('behavior === "pv")
      .window( Slide over 1.hours every 5.minutes on 'ts as 'sw )
      .groupBy( 'itemId, 'sw)
      .select( 'itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt )

    tableEnv.createTemporaryView("aggtable", aggTable, 'itemId, 'windowEnd, 'cnt)
    val resultTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |  select
        |    *,
        |    row_number()
        |      over (partition by windowEnd order by cnt desc)
        |      as row_num
        |    from aggtable )
        |where row_num <= 5
      """.stripMargin)

    tableEnv.createTemporaryView("datatable", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |  select
        |    *,
        |    row_number()
        |      over (partition by windowEnd order by cnt desc)
        |      as row_num
        |    from (
        |      select
        |        itemId,
        |        hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd,
        |        count(itemId) as cnt
        |      from datatable
        |      where behavior = 'pv'
        |      group by
        |        itemId,
        |        hop(ts, interval '5' minute, interval '1' hour)
        |    )
        |)
        |where row_num <= 5
      """.stripMargin)

    resultTable.toRetractStream[Row].print()

    env.execute("hot items sql job")
  }
}
