package com.example.job.bat

import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool

object BookCount {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.createLocalEnvironment(1)
    val data = Array("Hadoop: 1", "Spark: 2", "Hadoop: 3", "Spark: 5")
    val loadDS = env.fromCollection(data)
    loadDS.print()
    val splitDS = loadDS.map(_.split(":"))
    splitDS.print()
    val tuppleDS = splitDS.map({l => (l(0), l(1).trim.toInt)})
    tuppleDS.print()
    val sumData = tuppleDS.groupBy(0).sum(1)
    sumData.print()
  }

}
