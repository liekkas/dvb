package com.citic.guoan.dvb.live

import java.util.Calendar

import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/11/10.
  */
object CalcTimeInUse {
    case class LIVE(uid:String,date:String,channel_name:String,program_name:String)
    case class LEFT_DATA(row:Long,uid:String,month:Int,week:Int,day:String,hour:Int,
                         milliseconds:Long,channel_name:String,program_name:String)
    case class RIGHT_DATA(row:Long,uid:String,milliseconds:Long)

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("CalcTimeInUse")
      conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      conf.registerKryoClasses(Array(classOf[LIVE],classOf[LEFT_DATA],classOf[RIGHT_DATA]))

      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      //获取过滤数据
      val origin = sc.textFile(args(0))
        .map(_.split("\t"))
        .map(p => LIVE(p(0),p(1),p(2),p(3)))
      //获取关机数据
      val shutdown = sc.textFile(args(1))
        .filter(_ != "")
        .map(_.split("\t"))
        .map(p => LIVE(p(0),p(1),p(2),p(3)))

      val data = origin.union(shutdown)
        .sortBy(p => p.uid + "," + p.date)
      
      data.persist(StorageLevel.MEMORY_AND_DISK_SER)

      val leftT = data.zipWithIndex()
        .map(p => {
          val dateStr = p._1.date
          val index = p._2.toLong
          val month = dateStr.substring(0,6).toInt
          val day = dateStr.substring(0,4) + "-" + dateStr.substring(4,6) + "-" + dateStr.substring(6,8)
          val hour = dateStr.substring(8,10).toInt
          val calendar = DateUtils.toCalendar(DateUtils.parseDate(day,"yyyy-MM-dd"))
          calendar.setMinimalDaysInFirstWeek(4) // For ISO 8601
          val weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR)
          val week = dateStr.substring(0,4) + (if(weekOfYear>9) weekOfYear else "0"+weekOfYear)
          //绝对毫秒
          val mis = DateUtils.parseDate(dateStr, "yyyyMMddHHmmss").getTime
          LEFT_DATA(index,p._1.uid,month,week.toInt,day,hour,mis,p._1.channel_name,p._1.program_name)
        }).toDF()

      val rightT = data.zipWithIndex()
        .map(p => {
          val index = p._2.toLong
          //绝对毫秒
          val mis = DateUtils.parseDate(p._1.date, "yyyyMMddHHmmss").getTime
          RIGHT_DATA(index-1,p._1.uid,mis) //数据错位
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

      //去掉小于5秒和大于10小时的数据和关机数据
      df.filter((df("channel_name") !== "_") && (df("time_in_use") >= 5 && df("time_in_use") <= 36000))
        .registerTempTable("filter_result")

      sqlContext.sql(
        """
        select a.uid,a.month,a.week,a.day,a.hour,a.channel_name,a.program_name,sum(a.time_in_use)
          from filter_result a
         group by a.uid,a.month,a.week,a.day,a.hour,a.channel_name,a.program_name
        """.stripMargin)
        .map(f => f(0) + "\t" +f(1) + "\t" +f(2) + "\t" +f(3) + "\t" +f(4) + "\t" +f(5) + "\t" +
          f(6) + "\t" + f(7).toString.toDouble.toInt)
        .coalesce(args(3).toInt)
//        .saveAsTextFile(args(2))
        .saveAsTextFile(args(2), classOf[GzipCodec])
    }
  }


