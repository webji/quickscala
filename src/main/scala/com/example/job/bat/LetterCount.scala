package com.example.job.bat

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object LetterCount {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    val data = Array("Aa bbb c", "ccc bbb aaaAA")
    val env = ExecutionEnvironment.createLocalEnvironment(1)
    val flatMapped = env.fromCollection(data).flatMap{ _.split("") }.map { (_, 1) }.groupBy(0)

//    val flatMapped = env.fromCollection(data.split("")).map(l => (l, 1)).groupBy(0).sum(1)
//    flatMapped.print()
  }

}
