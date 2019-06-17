package com.kobekun.spark.project.dao

import com.kobekun.spark.project.domain.CourseClickCount
import com.kobekun.spark.utils.HbaseUtils
import org.apache.hadoop.hbase.HBaseCommonTestingUtility
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 实战课程点击数数据访问层
  */
object CourseClickCountDao {

  val tableName = "imooc_course_clickcount"

  val cf = "info" //一个cf可以放很多列

  val qualifer = "click_count"

  /**
    * 保存数据到HBASE中
    * @param list CourseClickCount集合
    */
  def save(list: ListBuffer[CourseClickCount]): Unit ={

    //将HBASE中的表名，转化成可操作的对象
    val table = HbaseUtils.getInstance().getTable(tableName)

    for(ele <- list){

      //对表进行增量插入
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),

        Bytes.toBytes(cf),

        Bytes.toBytes(qualifer),

        ele.click_count)
    }

  }

  /**
    * 根据rowkey查询值
    * @param day_course 天_课程ID
    * @return 点击数
    */
  def count(day_course: String): Long = {

    val table = HbaseUtils.getInstance().getTable(tableName)

    //根据rowkey获取get
    val get = new Get(Bytes.toBytes(day_course))

    //通过get获取结果，再从结果中取出value值(byte[])
    val value = table.get(get).getValue(cf.getBytes(),qualifer.getBytes())

    if(value == null){

      0l
    }else{
      //将byte转化成long值
      Bytes.toLong(value)
    }

  }

  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[CourseClickCount]

    list.append(CourseClickCount("20181111_8",10))
    list.append(CourseClickCount("20181111_9",20))
    list.append(CourseClickCount("20181111_1",100))

    save(list)

    println(count("20181111_1") + " : " +
      count("20181111_8") + " : " +
      count("20181111_9"))
  }
}

//进入HBASE  ./hbase shell