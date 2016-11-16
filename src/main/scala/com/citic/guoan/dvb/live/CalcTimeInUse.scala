package com.citic.guoan.dvb.live

import java.util.Calendar

import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.io.compress.{GzipCodec}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/11/10.
  */
object CalcTimeInUse {
    case class LEFT_DATA(row:Long,uid:String,month:Int,week:Int,day:String,hour:Int,
                         milliseconds:Long,channel_name:String,program_name:String)
    case class RIGHT_DATA(row:Long,uid:String,milliseconds:Long)

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("CalcTimeInUse")
      conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      conf.registerKryoClasses(Array(classOf[LEFT_DATA],classOf[RIGHT_DATA]))

      val sc = new SparkContext(conf)
      sc.setLogLevel("WARN")
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      //获取已排序好的数据
      val origin = sc.textFile(args(0))
      origin.cache()

      //去掉每个分区的第一行数据,用于配对相减
//      val pair = origin.mapPartitions(iter => iter.drop(1))
      val first = origin.first()
      val pair = origin.filter(_!=first)

      val leftT = origin.zipWithIndex()
        .map(p => {
          val content = p._1.toString.split("\t")
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

      val rightT = pair.zipWithIndex()
        .map(p => {
          val content = p._1.split("\t")
          val index = p._2.toLong
          //绝对毫秒
          val mis = DateUtils.parseDate(content(1), "yyyyMMddhhmmss").getTime
          RIGHT_DATA(index,content(0),mis)
        }).toDF()

      leftT.registerTempTable("left_table")
      rightT.registerTempTable("right_table")

//      leftT.show()
//      rightT.show()

      val df = sqlContext.sql(
        """
        select a.uid,a.month,a.week,a.day,a.hour,a.channel_name,a.program_name,
               (b.milliseconds-a.milliseconds)/1000 time_in_use
          from left_table a,right_table b
         where a.row = b.row
           and a.uid = b.uid
        """.stripMargin)

//      df.show()

      df.filter(df("time_in_use") >= 5 && df("time_in_use") <= 36000)  //去掉小于5秒和大于10小时的数据
        .registerTempTable("filter_result")

      sqlContext.sql(
        """
        select a.uid,a.month,a.week,a.day,a.hour,a.channel_name,a.program_name,sum(a.time_in_use)
          from filter_result a
         group by a.uid,a.month,a.week,a.day,a.hour,a.channel_name,a.program_name
        """.stripMargin)
        .map(f => f(0) + "\t" +f(1) + "\t" +f(2) + "\t" +f(3) + "\t" +f(4) + "\t" +f(5) + "\t" +
          f(6) + "\t" + f(7).toString.toDouble.toInt)
        .coalesce(args(2).toInt).saveAsTextFile(args(1), classOf[GzipCodec])
    }
  }
