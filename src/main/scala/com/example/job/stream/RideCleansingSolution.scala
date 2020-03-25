package com.example.job.stream

import com.example.source.stream.TaxiRideSource
import com.example.util.{DataUtil, GeoUtil}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object RideCleansingSolution {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", DataUtil.pathToRideData)

    val maxDelay = 60
    val speed = 600

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(DataUtil.parallelism)

    val rides = env.addSource(DataUtil.rideSourceOrTest(new TaxiRideSource(input, maxDelay, speed)))

    val filteredRides = rides.filter(r => GeoUtil.isInNYC(r.startLon, r.startLat) && GeoUtil.isInNYC(r.endLon, r.endLat))

    DataUtil.printOrTest(filteredRides)

    env.execute("Taxi Ride Cleansing")
  }
}
