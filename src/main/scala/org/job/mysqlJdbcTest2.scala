package org.job

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.customSink.{CustomMysqlJdbcSink2, MultiThreadConsumerSink}
import org.customSource.{CustomMySqlJdbcSource, CustomMySqlJdbcSource2}
import org.data.CustomUser

import scala.collection.mutable.ListBuffer

object mysqlJdbcTest2 {
  def main(array: Array[String]) {
    val time1=System.currentTimeMillis()
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    val jarFilePath = "C:\\Users\\Tomotaku\\Documents\\code\\flinkTest\\out\\artifacts\\flinkTest_jar\\flinkTest.jar"
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
    //      .createRemoteEnvironment(
    //        "10.133.76.5",
    //        8081,
    //        jarFilePath
    //        )
//    env.enableCheckpointing(1000)
    env.setRestartStrategy(RestartStrategies.noRestart())
    env.setParallelism(1)
//    env.getConfig.setAutoWatermarkInterval(5000)
    val stream: DataStream[ListBuffer[CustomUser]] = env.addSource(new CustomMySqlJdbcSource2)
    val time2=System.currentTimeMillis()
    stream.addSink(new CustomMysqlJdbcSink2()).setParallelism(4)
    env.execute("test")
    val time3=System.currentTimeMillis()
    println("Source Time: ",time2-time1)
    println("Sink Time: ",time3-time2)
    println("Total Time: ",time3-time1)
  }
}