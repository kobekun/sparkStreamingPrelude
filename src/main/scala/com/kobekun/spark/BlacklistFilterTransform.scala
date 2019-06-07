package com.kobekun.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 过滤黑名单
  */
object BlacklistFilterTransform {

  def main(args: Array[String]): Unit = {

    val sconf = new SparkConf().setAppName("BlacklistFilterTransform").setMaster("local[2]")

    //创建StreamingContext需要两个参数：SparkConf和batch interval
    val scont = new StreamingContext(sconf,Seconds(10))

    //黑名单
    val black = List("zs","ls")
    //将list转化成序列化的RDD，然后再map算子操作(_,true)
    val blackRDD = scont.sparkContext.parallelize(black).map((_,true))

    val lines = scont.socketTextStream("localhost",6789)

//    leftjoin
//    (zs: [<20190607, zs>, <true>])   xx
//      (ls: [<20190607, ls>, <true>])   xx
//        (ww: [<20190607, ww>, <false>])

//    transform: Return a new DStream by applying a RDD-to-RDD function
//    to every RDD of the source DStream.
//    This can be used to do arbitrary RDD operations on the DStream.

//    通过将RDD-to-RDD函数应用于源DStream的每个RDD来返回新的DStream。
//    这可以用于在DStream上执行任意RDD操作。
    val clicklog = lines.map(x => (x.split(",")(1),x))
        .transform(rdd => {
          rdd.leftOuterJoin(blackRDD).filter(x => x._2._2.getOrElse(false) != true)
            .map(x => x._2._1)
        })

    clicklog.print()

    scont.start()

    scont.awaitTermination()
  }
}


//怎么实现黑名单过滤？
//黑名单一般存储在数据库中，将数据库中的黑名单取出来，
// 将其通过sparkContext的parallelize将其转化成rdd，再对该rdd做个黑白名单的区别(true),
// 然后将过来的数据DStream进行map算子等操作，再用transform算子作用于DStream，
// 对之前的黑名单RDD进行操作，最后返回想要的结果