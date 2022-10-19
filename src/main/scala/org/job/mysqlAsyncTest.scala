package org.job

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala.createTypeInformation
import org.customAsync.MysqlAsyncRichMapFunction
import org.customSource.CustomMySqlJdbcSource
import org.data.CustomUser

import java.util.concurrent.TimeUnit

object mysqlAsyncTest {
  def main(array: Array[String]) {
    val maxIORequestCount: Int = 20
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setRestartStrategy(RestartStrategies.noRestart())

    val dataStream: DataStream[CustomUser] = env.addSource(new CustomMySqlJdbcSource)

    val dataStreamAsync: DataStream[String] = AsyncDataStream.orderedWait(
      dataStream,
      new MysqlAsyncRichMapFunction(),
      500000L,
      TimeUnit.MILLISECONDS,
      maxIORequestCount
    )
    dataStreamAsync.print()
    env.execute("Async Test")
  }
}
