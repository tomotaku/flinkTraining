package org.job

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.AsyncDataStream
//import org.apache.flink.streaming.api.datastream
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala.createTypeInformation
import org.customAsync.MysqlAsyncRichMapFunction
import org.customSink.{CustomMysqlJdbcSink, MultiThreadConsumerSink}
import org.customSource.CustomMySqlJdbcSource
import org.data.user

import java.util.concurrent.TimeUnit

object mysqlAsyncTest extends App {

  val maxIORequestCount: Int = 20
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setRestartStrategy(RestartStrategies.noRestart())

  val dataStream: DataStream[user] = env.addSource(new CustomMySqlJdbcSource)

  val dataStreamAsync: DataStream[(user, user)] = AsyncDataStream.orderedWait(
    dataStream,
    new MysqlAsyncRichMapFunction(),
    500000L,
    TimeUnit.MILLISECONDS,
    maxIORequestCount
  )
  dataStreamAsync.print()
  env.execute("Async Test")
}
