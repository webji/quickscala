package com.example.job.state

import java.util.concurrent.TimeUnit

import com.example.entity.TaxiRide
import com.example.source.stream.CheckpointedTaxiRideSource
import com.example.util.{DataUtil, GeoUtil, TravelTimePredictionModel}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

object TravelTimePrediction {
  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", DataUtil.pathToRideData)

    val speed = 600

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)))

    val rides = env.addSource(new CheckpointedTaxiRideSource(input, speed)).name("rides-checkpointed-source")
    val filteredRides = rides.filter(r => GeoUtil.isInNYC(r.startLon, r.startLat) && GeoUtil.isInNYC(r.endLon, r.endLat))
    val mappedRides = filteredRides.map(r => (GeoUtil.mapToGridCell(r.endLon, r.endLat), r))
    val keyedRides = mappedRides.keyBy(_._1)
    val flattedRides = keyedRides.flatMap(new PredictionModel())
    flattedRides.print()

    env.execute("Travel Time Prediction")
  }

  class PredictionModel extends RichFlatMapFunction[(Int, TaxiRide), (Long, Int)] {
    var modelState: ValueState[TravelTimePredictionModel] = _
    override def flatMap(value: (Int, TaxiRide), out: Collector[(Long, Int)]): Unit = {
      val model: TravelTimePredictionModel = Option(modelState.value).getOrElse(new TravelTimePredictionModel)
      val ride: TaxiRide = value._2

      val distance = GeoUtil.getEuclideanDistance(ride.startLon, ride.startLat, ride.endLon, ride.endLat)
      val direction = GeoUtil.getDirectionAngle(ride.endLon, ride.endLat, ride.startLon, ride.startLat)

      if (ride.isStart) {
        val predictedTime: Int = model.predictTravelTime(direction, distance)
        out.collect((ride.rideId, predictedTime))
      } else {
        val travelTime = (ride.endTime.getMillis - ride.startTime.getMillis) / 60000.0
        model.refineModel(direction, distance, travelTime)
        modelState.update(model)
      }
    }

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[TravelTimePredictionModel](
        "regressionModel",
        TypeInformation.of(classOf[TravelTimePredictionModel])
      )
      modelState = getRuntimeContext.getState(descriptor)
    }
  }
}
