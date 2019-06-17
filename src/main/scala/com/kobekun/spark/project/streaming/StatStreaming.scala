package com.kobekun.spark.project.streaming

import com.kobekun.spark.project.dao.{CourseClickCountDao, CourseSearchClickCountDao}
import com.kobekun.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.kobekun.spark.project.utils.TimeUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object StatStreaming {

  def main(args: Array[String]): Unit = {

    if(args.length != 4){

      System.err.println("usage: <zkQuorum> <groupid> <topics> <numThreads>")

      System.exit(1)
    }

    val Array(zkQuorum, groupid, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("StatStreaming").setMaster("local[4]")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val topicMap = topics.split(",").map((_,1)).toMap

    val kafkaMsg = KafkaUtils.createStream(ssc, zkQuorum, groupid, topicMap)

    //测试一：测试数据接收多少条
//    kafkaMsg.map(_._2).count().print()

    //测试二：数据清洗
//    167.168.87.143  2019-06-15 16:40:01     "GET /course/list HTTP/1.1"     500     https://www.sogou.com/web?query=Spark Streaming实战
//    132.98.187.63   2019-06-15 16:40:01     "GET /class/112.html HTTP/1.1"  200     http://cn.bing.com/search?q=Spark Streaming实战
    val logs = kafkaMsg.map(_._2)

    val cleanData = logs.map(line => {
      val infos = line.split("\t")

      val url = infos(2).split(" ")(1)

      var courseId = 0

      if(url.startsWith("/class")){

        val courseHTML = url.split("/")(2)

        courseId = courseHTML.substring(0, courseHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), TimeUtils.parseToTarget(infos(1)), courseId, infos(3).toInt, infos(4))

    }).filter(clicklog => clicklog.courseId != 0)

//    cleanData.print()

    //测试三：统计今天到现在为止实战课程的访问量

    cleanData.map(x => {

      (x.time.substring(0,8) + "_" + x.courseId, 1)

    }).reduceByKey(_+_).foreachRDD(rdd => {

      rdd.foreachPartition(partitionRecords => {

        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {

          list.append(CourseClickCount(pair._1, pair._2))

        })

        CourseClickCountDao.save(list)
      })
    })


    //测试四：统计今天到现在为止实战课程的搜索引擎过来的访问量

    cleanData.map(x => {

//      把https://www.sogou.com/web?query=Spark Streaming实战 转成
      //      https:/www.sogou.com/web?query=Spark Streaming实战

      val referer = x.referer.replaceAll("//", "/")

      val splits = referer.split("/")

      var search = ""

      if(splits.length > 2){

        search = splits(1)

      }

      (search, x.courseId, x.time)

    }).filter(x => x._1 != "").map(x =>

      (x._3.substring(0,8) + "_" + x._1 + "_" + x._2, 1)

    ).reduceByKey(_+_).foreachRDD(rdd => {

      rdd.foreachPartition(partitionRecords => {

        val list = new ListBuffer[CourseSearchClickCount]

        partitionRecords.foreach(pair => {

          list.append(CourseSearchClickCount(pair._1, pair._2))

        })

        CourseSearchClickCountDao.save(list)
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }
}

//flume接收日志产生器产生的日志时，可能出现以下的报错信息：
//
//Space for commit to queue couldn't be acquired. Sinks are likely not keeping up with sources,
//or the buffer size is too tight
//无法获取提交队列的空间。 接收器可能无法跟上源，或者缓冲区大小太紧
//
//说明缓冲区较小，不足以接收太多的日志数据。而且，kafka接收器接收能力也跟不上源产生的数据

//清洗结果

//ClickLog(29.187.55.72,20190615191701,145,500,-)
//ClickLog(55.46.30.167,20190615191701,131,200,-)
//ClickLog(72.98.156.124,20190615191701,130,404,-)
//ClickLog(98.124.87.10,20190615191701,146,404,-)
//ClickLog(72.168.30.55,20190615191701,145,404,-)
//ClickLog(168.55.46.167,20190615191701,146,200,-)
//ClickLog(87.156.168.30,20190615191701,128,404,-)
//ClickLog(124.30.156.132,20190615191701,112,404,https://www.sogou.com/web?query=Spark Streaming实战)
//ClickLog(72.87.132.46,20190615191701,146,404,-)
//ClickLog(46.143.124.156,20190615191701,131,200,-)