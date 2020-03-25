package com.example.job.table

import java.io.FileInputStream

import com.example.driver.MysqlStreamSink
import com.example.entity.source.{KafkaSource, MysqlSource, SourceType}
import com.example.entity.table.{KafkaSourceTable, MysqlDimTable, MysqlSinkTable, OffsetType, SchemaHelper, SourceTableBase, TimeType}
import com.example.file.{SqlConf, SqlFile}
import com.example.util.PathUtil
import org.apache.flink.api.java.io.jdbc.JDBCTableSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic

import scala.collection.JavaConversions._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors
import org.apache.flink.table.descriptors.{Json, Kafka}
import org.apache.flink.types.Row

object GeneralSql {
  var timeCharacteristic : TimeCharacteristic = TimeCharacteristic.ProcessingTime

  def connectKafkaSourceTable(tableEnvironment: StreamTableEnvironment, source: KafkaSource, table: KafkaSourceTable): Unit = {
    val kafka = new Kafka().version(source.version)
      .topic(table.getTopic)
      .property("bootstrap.servers", source.getBrokers)
      .property("zookeeper.connect", source.getAddress)
    if (table.getOffset == OffsetType.EARLIEST) {
      kafka.startFromEarliest()
    } else {
      kafka.startFromLatest()
    }

    timeCharacteristic = table.getTimeType()

    tableEnvironment.connect(kafka)
      .inAppendMode()
      .withFormat(new Json().failOnMissingField(false).deriveSchema())
      .withSchema(SchemaHelper.fromList(table.getSchema))
      .registerTableSource(table.getTableName)
  }

  def connectMysqlTable(tableEnvironment: StreamTableEnvironment, source: MysqlSource, table: MysqlDimTable): Unit = {
    val tableSource :JDBCTableSource = table.jdbcTableSource()
    tableEnvironment.registerTableSource(table.getMapper, tableSource)
    tableEnvironment.registerFunction(table.getMapper, tableSource.getLookupFunction(Array(table.getPk)))
  }

  def connectSourceTables(tableEnvironment: StreamTableEnvironment, sourceTables: List[com.example.entity.table.Table]): Unit = {
    for (sourceTable <- sourceTables) {
      val source = sourceTable.getSource
      val sourceType = source.getType
      if (sourceType == SourceType.KAFKA_UNIVERSAL) {
        val kafkaSource: KafkaSource = source.asInstanceOf[KafkaSource]
        val kafkaSourceTable: KafkaSourceTable = sourceTable.asInstanceOf[KafkaSourceTable]
        connectKafkaSourceTable(tableEnvironment, kafkaSource, kafkaSourceTable)
      }
    }
  }

  def connectDimTables(tableEnvironment: StreamTableEnvironment, dimTables: List[com.example.entity.table.Table]): Unit = {
    for (dimTable <- dimTables) {
      val source = dimTable.getSource
      val sourceType = source.getType
      if (sourceType == SourceType.MYSQL) {
        val mysqlSource : MysqlSource = source.asInstanceOf[MysqlSource]
        val mysqlDimTable : MysqlDimTable = dimTable.asInstanceOf[MysqlDimTable]
        connectMysqlTable(tableEnvironment, mysqlSource, mysqlDimTable)
      }
    }
  }

  def addSinks(dataStream: DataStream[Row], sinkTabls: List[com.example.entity.table.Table]): Unit = {
    for (sinkTable <- sinkTabls) {
      val source = sinkTable.getSource
      val sourceType = source.getType
      if (sourceType == SourceType.MYSQL) {
        val mysqlSource : MysqlSource = source.asInstanceOf[MysqlSource]
        val mysqlSinkTable : MysqlSinkTable = sinkTable.asInstanceOf[MysqlSinkTable]
        val mysqlSink = new MysqlStreamSink(mysqlSinkTable)
        dataStream.addSink(mysqlSink)
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val sqlFilePath = params.get("sql", "data/demo.sql")
    val sqlFile = new SqlFile(sqlFilePath)
    val sql = sqlFile.readSql()

    println(sql)

    if (sql.length <= 0) {
      println("Failed to get SQL from : " + sqlFilePath + " with SQL: " + sql)
      return
    }

    val confFile = params.get("flinksql.json", "data/flinksql.json")
    val sqlConf: SqlConf = new SqlConf(confFile)
    val sourceTables  = sqlConf.getSourceTables
    val dimTables = sqlConf.getDimTables
    val sinkTables = sqlConf.getSinkTables

    import org.apache.flink.table.api.EnvironmentSettings
    val settings = EnvironmentSettings.newInstance.inStreamingMode.useBlinkPlanner.build
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    connectSourceTables(tEnv, sourceTables.toList)
    connectDimTables(tEnv, dimTables.toList)

    val results: Table = tEnv.sqlQuery(sql)
    val dataStream = tEnv.toAppendStream[Row](results)

    addSinks(dataStream, sinkTables.toList)
//    dataStream.print()

    env.setStreamTimeCharacteristic(timeCharacteristic)
    env.setParallelism(1)
    env.execute()
  }

}
