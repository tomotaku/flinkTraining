package org.job

import com.ververica.cdc.connectors.mysql.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import com.ververica.cdc.debezium.{DebeziumSourceFunction, StringDebeziumDeserializationSchema}

object mysqlJdbcTest_cdc {
  def main(array: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val mySqlSource = MySqlSource.builder()
      .hostname("localhost")
      .port(3306)
      .username("root")
      .password("123456")
      .databaseList("test")
      .tableList("test.flinkSourceTest")
      .deserializer(new StringDebeziumDeserializationSchema())
      .startupOptions(StartupOptions.initial())
      .build()

    env.enableCheckpointing(3000)
    env.setRestartStrategy(RestartStrategies.noRestart())
//    env.setStateBackend()
    env.setParallelism(1)
    val stream: DataStream[String] = env.addSource(mySqlSource)
    stream.print()
    env.execute("test")
  }
}