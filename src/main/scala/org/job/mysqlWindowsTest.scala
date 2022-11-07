package org.job

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.customProcessFunc.customProcessFunc
import org.customSource.CustomMySqlJdbcSource
import org.data.CustomUser

object mysqlWindowsTest {
  def main(array: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.noRestart())
    env.setParallelism(1)
    val stream: DataStream[CustomUser] = env.addSource(new CustomMySqlJdbcSource)
    stream.keyBy(custom => custom.id).process(new customProcessFunc).print()
    env.execute("test")
  }
}
