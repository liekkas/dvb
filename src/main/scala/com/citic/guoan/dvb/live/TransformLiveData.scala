package com.citic.guoan.dvb.live

import java.util.Calendar

import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/10/26.
  */
object TransformLiveData {
  case class LEFT_DATA(row:Long,uid:String,month:Int,week:Int,day:String,hour:Int,
                       milliseconds:Long,channel_name:String,program_name:String)
  case class RIGHT_DATA(row:Long,uid:String,milliseconds:Long)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransformLiveData")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val origin = sc.textFile(args(0)) //分解出地区|采集系统ID|内容
    origin.cache()

    val count = origin.count()
    val first = origin.first()

    val leftT = origin.zipWithIndex().filter(_._2 != count-1)
      .map(p => {
        val content = p._1.split("\t")
        val index = p._2.toLong
        val month = content(1).substring(0,6).toInt
        val day = content(1).substring(0,4) + "-" + content(1).substring(4,6) + "-" + content(1).substring(6,8)
        val hour = content(1).substring(8,10).toInt
        val calendar = DateUtils.toCalendar(DateUtils.parseDate(day,"yyyy-MM-dd"))
        calendar.setMinimalDaysInFirstWeek(4) // For ISO 8601
        val weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR)
        val week = content(1).substring(0,4) + (if(weekOfYear>9) weekOfYear else "0"+weekOfYear)
        //绝对毫秒
        val mis = DateUtils.parseDate(content(1), "yyyyMMddhhmmss").getTime
        LEFT_DATA(index,content(0),month,week.toInt,day,hour,mis,content(2),content(3))
      }).toDF()

    val rightT = origin.filter(_ != first).zipWithIndex()
      .map(p => {
        val content = p._1.split("\t")
        val index = p._2.toLong
        //绝对毫秒
        val mis = DateUtils.parseDate(content(1), "yyyyMMddhhmmss").getTime
        RIGHT_DATA(index,content(0),mis)
      }).toDF()

    leftT.registerTempTable("left_table")
    rightT.registerTempTable("right_table")

    sqlContext.sql(
      """
        select a.uid,a.month,a.week,a.day,a.hour,a.channel_name,a.program_name,
               sum(b.milliseconds-a.milliseconds)/1000 time_in_use
          from left_table a,right_table b
         where a.row = b.row
           and a.uid = b.uid
         group by a.uid,a.month,a.week,a.day,a.hour,a.channel_name,a.program_name
      """.stripMargin)
      .map(f => f(0) + "\t" +f(1) + "\t" +f(2) + "\t" +f(3) + "\t" +f(4) + "\t" +f(5) + "\t" +
        f(6) + "\t" + f(7).toString.toDouble.toInt)
      .repartition(1).saveAsTextFile(args(1))
  }
}
