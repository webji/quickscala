package com.example.job.bat

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object TwoRoundSort {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    val data = Array("hadoop@apache    200", "hive@apache      500", "yarn@apache   580", "hive@apache  159"
      , "hadoop@apache  300", "hive@apache    258", "hadoop@apache   150", "yarn@apache    560", "yarn@apache  260")
    val env = ExecutionEnvironment.createLocalEnvironment(1)
    val loadData = env.fromCollection(data).map(_.replaceAll("\\s+", " "))
    val tuppleData = loadData.map(_.split(" ")).map {l => (l(0), List(l(1).trim.toInt))}
    val groupedDS = tuppleData.sortPartition(0, Order.DESCENDING).groupBy(0).reduce((l, r) => (l._1, (l._2 ++ r._2).sorted))
    groupedDS.print()
  }
}
