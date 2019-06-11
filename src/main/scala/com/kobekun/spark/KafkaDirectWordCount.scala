package com.kobekun.spark

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 对接kafka方式 direct
  */
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){

      //<>表示必须的参数  []表示可选参数
      System.err.println("usage: KafkaDirectWordCount <brokers> <topics>")

      System.exit(1)

    }

    val Array(brokers, topics) = args

    //打包时停掉后面的
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("KafkaReceiverWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(10))

    val kafkaParam = Map[String,String]("metadata.broker.list" -> brokers)

    val topicsSet = topics.split(",").toSet
    //kafka创建 DStream of (Kafka message key, Kafka message value)
    val msg = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParam, topicsSet)

    //工作中是对这块的开发   上面的流程按规范来就行
    msg.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()

    ssc.awaitTermination()
  }
}
