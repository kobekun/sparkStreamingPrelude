package com.kobekun.spark.project.domain

/**
  * 实战课程点击数实体类
  * @param day_course 对应HBASE中的rowkey, 20181111_120
  * @param click_count 对应20181111_120的点击总数
  */
case class CourseClickCount(day_course: String, click_count: Long)
