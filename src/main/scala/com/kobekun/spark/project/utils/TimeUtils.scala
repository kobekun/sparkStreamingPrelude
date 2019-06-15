package com.kobekun.spark.project.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object TimeUtils {

  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time: String): Long ={

    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime()
  }

  def parseToTarget(time: String): String ={
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {

    println(parseToTarget("2019-06-15 16:40:01"))
  }
}
