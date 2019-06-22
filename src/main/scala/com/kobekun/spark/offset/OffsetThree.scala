package com.kobekun.spark.offset

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

/**
  * 1、创建streamingcontext
  * 2、从kafka中获取数据 <== offset获取
  * 3、根据业务对数据进行处理
  * 4、将处理结果写入到外部存储中 <== offset保存
  * 5、启动程序等待程序终止
  */
object OffsetThree {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("OffsetOne").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf,Seconds(10))

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "hadoop000:9092",
      "auto.offset.reset" -> "smallest"
    )

    val topics = "imooc_pk_offset".split(",").toSet

    //获取偏移量  TopicAndPartition ==> case class定义构造器，详见源码  一个topic多个分区
    val fromOffsets = Map[TopicAndPartition, Long]()

    val messages = if(fromOffsets.size == 0){ //从头消费

      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,
        kafkaParams,topics)
    }else{

      val messageHandler = (mm: MessageAndMetadata[String,String]) => (mm.key(),mm.message())

//      messageHandler: MessageAndMetadata ==> case class
//      Return the decoded message key and payload 返回已解码的消息密钥和有效负载(message信息)
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc
            ,kafkaParams
            ,fromOffsets
          ,messageHandler)
    }


    messages.foreachRDD(rdd => {

//      业务逻辑  保存数据

      println("kobekun； " + rdd.count())

      /**
        * 提交offset HasOffsetRanges为特质
        *
        * trait HasOffsetRanges {
        * def offsetRanges: Array[OffsetRange]
        * }
        *
        * OffsetRange中保存topic、partition和起止偏移量  和inputDStream中的fromOffsets
        * ,messageHandler对应
        *
        * final class OffsetRange private(
        * val topic: String,
        * val partition: Int,
        * val fromOffset: Long,
        * val untilOffset: Long) extends Serializable {
        * import OffsetRange.OffsetRangeTuple
        */

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      offsetRanges.foreach(x => {
        //业务逻辑中此处应该是提交offset
        println(s"${x.topic} ${x.partition} ${x.fromOffset} ${x.untilOffset}")

      })

    })

    ssc.start()
    ssc.awaitTermination()
  }
}

//数据一致性
//保存offset -> A ->保存数据  如果A挂掉，(程序会认为这批数据已经保存，其实没有保存)这批数据会丢掉一些
//保存数据 -> B ->保存offset  如果B挂掉，这批数据会重复执行


//为了使数据一致：
//1、采用幂等性   to Mysql  用主键保存  upsert ==>  有数据update没有数据insert  不管执行多少次结果不变
//2、通过事务 要么全部成功，要么全部失败