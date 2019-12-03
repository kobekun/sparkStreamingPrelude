package com.kobekun.spark.offset

import java.util.{Date, Properties}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("serializer.class",ValueUtils.getStringValue("serializer.class"))
    properties.put("metadata.broker.list",ValueUtils.getStringValue("metadata.broker.list"))
    properties.put("request.required.acks",ValueUtils.getStringValue("request.required.acks"))
    val producerConfig = new ProducerConfig(properties)
    val producer = new Producer[String,String](producerConfig)
    val topic = ValueUtils.getStringValue("kafka.topics")
    //每次产生100条数据
    var i = 0
    for (i <- 1 to 100) {
      val runtimes = new Date().toString
      val messages = new KeyedMessage[String, String](topic,i+"","hlw: "+runtimes)
      producer.send(messages)
    }
    println("数据发送完毕...")
  }

}
