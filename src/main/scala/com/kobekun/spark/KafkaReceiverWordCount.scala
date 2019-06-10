package com.kobekun.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 对接kafka方式 receiver
  */
object KafkaReceiverWordCount {

  def main(args: Array[String]): Unit = {

    if(args.length != 4){

      //<>表示必须的参数  []表示可选参数
      System.err.println("usage: KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")

      System.exit(1)

    }

    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(10))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    //kafka创建ReceiverInputDStream流
    val msg = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    msg.print()

    msg.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()

    ssc.awaitTermination()
  }
}
