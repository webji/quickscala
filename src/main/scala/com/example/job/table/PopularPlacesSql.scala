package com.example.job.table

import com.example.source.table.TaxiRideTableSource
import com.example.util.{DataUtil, GeoUtil}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object PopularPlacesSql {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", DataUtil.pathToRideData)

    val maxEventDelay = 60
    val servingSpeedFactor = 600

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tEnv = StreamTableEnvironment.create(env)
    tEnv.registerTableSource("TaxiRides", new TaxiRideTableSource(input, maxEventDelay, servingSpeedFactor))
    tEnv.registerFunction("isInNYC", new GeoUtil.IsInNYC)
    tEnv.registerFunction("toCellId", new GeoUtil.ToCellId)
    tEnv.registerFunction("toCoords", new GeoUtil.ToCoords)

    val results: Table = tEnv.sqlQuery(
      """
        |SELECT
        | toCoords(cell), wstart, wend, isStart, popCnt
        |FROM
        | (SELECT
        |   cell,
        |   isStart,
        |   HOP_START(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS wstart,
        |   HOP_END(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS wend,
        |   COUNT(isStart) AS popCnt
        | FROM
        |   (SELECT
        |     eventTime,
        |     isStart,
        |     CASE WHEN isStart THEN toCellId(startLon, startLat) ELSE toCellId(endLon, endLat) END AS cell
        |    FROM TaxiRides
        |    WHERE isInNYC(startLon, startLat) AND isInNYC(endLong, endLat))
        |  GROUP BY cell, isStart, HOP(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE))
        |WHERE popCnt > 20
        |""".stripMargin
    )
    tEnv.toAppendStream[Row](results).print

    env.execute()
  }
}
