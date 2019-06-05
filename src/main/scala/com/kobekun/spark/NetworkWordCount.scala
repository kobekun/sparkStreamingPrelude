package com.kobekun.spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

/**
  * spark streaming 处理socket数据
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    //此处master设为local或者local[1]的后果
    // WARN streaming.StreamingContext: spark.master should be set as local[n],
    // n > 1 in local mode if you have receivers to get data,
    // otherwise Spark jobs will not get resources to process the received data.

    //因为receiver只有一个，只接受socket中的数据，放到内存中。
    // 数据被操作，也是需要资源的，也是需要receiver的。一个receiver相当于一个线程
    val sconf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")

    //创建StreamingContext需要两个参数：SparkConf和batch interval
    val scont = new StreamingContext(sconf,Seconds(5))

    val lines = scont.socketTextStream("localhost",6789)

    //reduceByKey合并相同键的值  groupByKey对相同键进行分组
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    scont.start()
    scont.awaitTermination()
  }
}

//Windows中下面报错不用管
//19/06/01 21:24:11 ERROR util.Shell: Failed to locate the winutils binary in the hadoop binary path
//java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
//at org.apache.hadoop.util.Shell.getQualifiedBinPath(Shell.java:378)