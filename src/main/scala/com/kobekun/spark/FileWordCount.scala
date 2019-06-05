package com.kobekun.spark


import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用sparkStreaming处理文件系统（本地/hdfs）的数据
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","E:\\hadoop-2.6.5")
    //由于是文件系统的数据，所以不用receiver
    val sconf = new SparkConf().setAppName("FileWordCount").setMaster("local")

    val scont = new StreamingContext(sconf, Seconds(10))

    val filepath = "file:///C:\\Users\\mouse\\Desktop\\kobekun"

    //返回值DStream, 将kobekun.txt移动到该目录下,这样的方式并不是以流的形式写入的

//    在cmd窗口，使用cp命令将文件复制到datas文件夹中，然后SparkStreaming即可读取其中的数据。
//
//    cp .\test.log .\datas\

    //而且经过测试 file:\\是不被支持的

    val lines = scont.textFileStream(filepath)


    val result = lines.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)

    result.print()

    scont.start()
    scont.awaitTermination()
  }
}
