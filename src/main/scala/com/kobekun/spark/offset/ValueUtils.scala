package com.kobekun.spark.offset

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils

object ValueUtils {

  val load = ConfigFactory.load()
  def getStringValue(key:String, defaultValue:String="") = {
    val value = load.getString(key)
    if(StringUtils.isNotEmpty(value)) {
      value
    } else {
      defaultValue
    }
  }
}
