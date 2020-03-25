/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.job.stream

import com.example.source.stream.TaxiRideSource
import com.example.util.DataUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._



object TaxiRiderCount {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", DataUtil.pathToRideData)

    val maxEventDelay = 60
    val servingSpeedFactor = 600
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(DataUtil.parallelism)

    val rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)).name("taxiride-source")
    val tuples = rides.map(_.driverId).map{(_, 1L)}

    val keydByDriverId = tuples.keyBy(0)
    val count = keydByDriverId.sum(1)
    count.print()

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
