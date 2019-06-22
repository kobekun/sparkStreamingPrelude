package com.kobekun.spark.offset

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OffsetOne {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("OffsetOne").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "hadoop000:9092",
      //本应用程序如果中断，下次启动时偏移量从头开始消费
      "auto.offset.reset" -> "smallest"
    )

    val topics = "imooc_pk_offset".split(",").toSet

    val msg = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,
      kafkaParams,topics)

    msg.foreachRDD(rdd => {

      if(!rdd.isEmpty()){

        println("kobekun: " + rdd.count())

      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
