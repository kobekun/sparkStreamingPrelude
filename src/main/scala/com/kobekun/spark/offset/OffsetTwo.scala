package com.kobekun.spark.offset

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object OffsetTwo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("OffsetOne").setMaster("local[2]")

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "hadoop000:9092",
      //本应用程序如果中断，下次启动时偏移量从头开始消费
      "auto.offset.reset" -> "smallest"
    )

    val topics = "imooc_pk_offset".split(",").toSet

    val checkpointDirectory = "hdfs://hadoop000:8020/offset"
    // Function to create and setup a new StreamingContext
    def functionToCreateContext(): StreamingContext = {

       val ssc = new StreamingContext(sparkConf, Seconds(10))   // new context

      val msg = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,
        kafkaParams,topics)

      //设置checkpoint
      ssc.checkpoint(checkpointDirectory)   // set checkpoint directory

      msg.checkpoint(Duration(10*1000))

      msg.foreachRDD(rdd => {

        if(!rdd.isEmpty()){

          println("kobekun: " + rdd.count())

        }
      })

      ssc
    }

    //代码发生变动用这种方式不合适  如果数据在checkpointDirectory中存在，就用这数据构建streamingCOntext
//    ,如果不存在，就用functionToCreateContext这个函数创建

    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

    ssc.start()
    ssc.awaitTermination()
  }
}
