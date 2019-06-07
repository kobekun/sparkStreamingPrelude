package com.kobekun.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

/**
  * 打包到Linux上操作
  * sparksql 和sparkstreaming互操作词频统计
  */
object SparkSQLNetworkWordcount {

  def main(args: Array[String]): Unit = {

    val sconf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")

    val scont = new StreamingContext(sconf,Seconds(10))

    val lines = scont.socketTextStream("localhost",6789)

    val words = lines.flatMap(_.split(" "))

    // Convert RDDs of the words DStream to DataFrame and run SQL query
    words.foreachRDD { (rdd: RDD[String], time: Time) =>

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

      import spark.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      // Creates a temporary view using the DataFrame
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    }

    scont.start()
    scont.awaitTermination()
  }

  /** Case class for converting RDD to DataFrame */
  case class Record(word: String)


  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
// DStream -> RDD -> DataFrame(spark sql) -> show |
//DataFrame
//+----+-----+
//|word|total|
//+----+-----+
//|   d|    1|
//|   c|    3|
//|   b|    2|
//|   a|    5|
//|   s|    1|
//+----+-----+