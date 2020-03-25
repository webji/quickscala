package com.example.job.cep

import com.example.entity.TaxiRide
import com.example.source.stream.CheckpointedTaxiRideSource
import com.example.util.DataUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.Map

object LongRidesCEPSolution {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", DataUtil.pathToRideData)

    val speed = 60

    val env = StreamExecutionEnvironment.createLocalEnvironment(DataUtil.parallelism)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(DataUtil.parallelism)

    val rides = env.addSource(DataUtil.rideSourceOrTest(new CheckpointedTaxiRideSource(input, speed)))

    val keyedRides = rides.keyBy(_.rideId)

    val completedRides = Pattern.begin[TaxiRide]("start").where(_.isStart).next("end").where(!_.isStart)

    val patternStream: PatternStream[TaxiRide] = CEP.pattern[TaxiRide](keyedRides, completedRides.within(Time.minutes(120)))

    val timedoutTag = new OutputTag[TaxiRide]("timedout")

    val timeoutFunction = (map: Map[String, Iterable[TaxiRide]], timestamp: Long, out: Collector[TaxiRide]) => {
      val rideStarted = map.get("start").get.head
      out.collect(rideStarted)
    }

    val selectFunction = (map: Map[String, Iterable[TaxiRide]], out: Collector[TaxiRide]) => {

    }

    val longRides = patternStream.flatSelect(timedoutTag)(timeoutFunction)(selectFunction)

    DataUtil.printOrTest(longRides.getSideOutput(timedoutTag))

    env.execute("Long Taxi Rides (CEP)")
  }

}
