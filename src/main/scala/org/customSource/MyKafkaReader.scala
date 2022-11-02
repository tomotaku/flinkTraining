package org.customSource

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{createTuple2TypeInformation, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class MyKafkaReader extends KafkaDeserializationSchema[(String, String)] {
  //是否流结束
  override def isEndOfStream(nextElement: (String, String)): Boolean = {
    false
  }

  //反序列化
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
    if (record != null) {
      var key = "null"
      var value = "null"
      if (record.key() != null) {
        key = new String(record.key(), "UTF-8")
      }
      if (record.value() != null) { //从Kafka记录中得到Value
        value = new String(record.value(), "UTF-8")
      }
      (key, value)
    } else { //数据为空
      ("null", "nulll")
    }
  }

  override def getProducedType: TypeInformation[(String, String)] = {
    createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
  }
}
