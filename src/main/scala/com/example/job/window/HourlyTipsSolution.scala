package com.example.job.window

import com.example.entity.TaxiFare
import com.example.source.stream.TaxiFareSource
import com.example.util.DataUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HourlyTipsSolution {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", DataUtil.pathToFareData)

    val maxDelay = 60
    val speed = 600

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(DataUtil.parallelism)

    val fares = env.addSource(DataUtil.fareSourceOrTest(new TaxiFareSource(input, maxDelay, speed)))

    val hourlyTips = fares.map((f: TaxiFare) => (f.driverId, f.tip)).keyBy(_._1)
      .timeWindow(Time.hours(1)).reduce((f1: (Long, Float), f2: (Long, Float)) => { (f1._1, f1._2 + f2._2)},
      new WrapWithWindowInfo())

    val hourlMax = hourlyTips.timeWindowAll(Time.hours(1)).maxBy(2)

    DataUtil.printOrTest(hourlMax)

    env.execute("Hourly Tips (scala)")
  }

  class WrapWithWindowInfo() extends ProcessWindowFunction[(Long, Float), (Long, Long, Float), Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[(Long, Float)], out: Collector[(Long, Long, Float)]): Unit = {
      val sumOfTips = elements.iterator.next()._2
      out.collect((context.window.getEnd(), key, sumOfTips))
    }
  }

}
