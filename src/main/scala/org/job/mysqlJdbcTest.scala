package org.job

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.customSink.{CustomMysqlJdbcSink, MultiThreadConsumerSink}
import org.customSource.CustomMySqlJdbcSource
import org.data.user

object mysqlJdbcTest extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  //    val jarFilePath = "C:\\Users\\Tomotaku\\Documents\\code\\flinkTest\\out\\artifacts\\flinkTest_jar\\flinkTest.jar"
  //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
  //      .createRemoteEnvironment(
  //        "10.133.76.5",
  //        8081,
  //        jarFilePath
  //        )
  env.enableCheckpointing(300)
  env.setRestartStrategy(RestartStrategies.noRestart())
  env.setParallelism(1)
  val stream: DataStream[user] = env.addSource(new CustomMySqlJdbcSource)
  stream.addSink(new MultiThreadConsumerSink())
  env.execute("test")
}