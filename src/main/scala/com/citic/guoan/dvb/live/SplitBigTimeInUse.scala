package com.citic.guoan.dvb.live

import java.util.Calendar

import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/10/26.
  */
object SplitBigTimeInUse {
  case class LIVE(uid:String,date:String,channel_name:String,program_name:String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SplitBigTimeInUse")
    val sc = new SparkContext(conf)

    sc.textFile(args(0))
      .map(_.split("\t"))
      .map(p => {
        val timeInUse = p(7).toLong
        if(timeInUse > 3600) {
          val spans = Math.ceil(timeInUse * 1.0 / 3600).toInt
          val hourStr = if(p(4) < "10") "0"+p(4) else p(4)
          val date = DateUtils.parseDate(p(3)+hourStr,"yyyy-MM-ddHH")
          var result = ""
          (0 until spans).foreach(span => {
            val dateStr = DateFormatUtils.format(DateUtils.addHours(date,span),"yyyyMMddHH")
            val month = dateStr.substring(0,6).toInt
            val day = dateStr.substring(0,4) + "-" + dateStr.substring(4,6) + "-" + dateStr.substring(6,8)
            val hour = dateStr.substring(8,10).toInt
            val calendar = DateUtils.toCalendar(DateUtils.parseDate(day,"yyyy-MM-dd"))
            calendar.setMinimalDaysInFirstWeek(4) // For ISO 8601
            val weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR)
            val week = dateStr.substring(0,4) + (if(weekOfYear>9) weekOfYear else "0"+weekOfYear)
            val timeUse = if(span < spans-1) 3600 else (timeInUse - 3600 * span)
            result += p(0) + "\t" + month + "\t" + week + "\t" + day + "\t" +
              hour + "\t" + p(5) + "\t" + p(6) + "\t" + timeUse + "\n"
          })
          result
        } else {
          p(0) + "\t" + p(1) + "\t" + p(2) + "\t" + p(3) + "\t" +
            p(4) + "\t" + p(5) + "\t" + p(6) + "\t" + p(7)
        }
      })
      .coalesce(args(2).toInt)
//      .saveAsTextFile(args(1))
      .saveAsTextFile(args(1),classOf[GzipCodec])
  }
}
