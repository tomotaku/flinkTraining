package org.job

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.customSource.MyKafkaReader

import java.util.Properties

object kafkaTest {
  //2、导入隐式转换
  def main(args: Array[String]): Unit = {
    //1、初始化Flink流计算的环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //修改并行度
    streamEnv.setParallelism(1) //默认所有算子的并行度为1


    //连接Kafka的属性
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "Clusters")
    props.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset", "latest")

    //设置Kafka数据源
    val stream: DataStream[(String, String)] = streamEnv
      .addSource(new FlinkKafkaConsumer[(String, String)]("test", new MyKafkaReader, props))

    stream.print()

    streamEnv.execute()
  }
}