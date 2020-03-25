package com.example.job.table

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._


object KafkaRedisJoinSql {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val sql_file = params.getRequired("sql")

  }

}
