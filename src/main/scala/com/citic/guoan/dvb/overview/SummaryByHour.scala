package com.citic.guoan.dvb.overview

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/11/21.
  */
object SummaryByHour {
  case class SUMMARY(date_time:String,hour:String,user_num:Long,time_use_sum:Double)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SummaryByHour")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.textFile(args(0)).map(p => {
      val arr = p.split("\t")
      SUMMARY(arr(0),arr(1),arr(3).toLong,arr(5).toDouble)
    }).toDF().registerTempTable("live")

    sc.textFile(args(1)).map(p => {
      val arr = p.split("\t")
      SUMMARY(arr(0),arr(1),arr(3).toLong,arr(5).toDouble)
    }).toDF().registerTempTable("demand")

    sqlContext.sql(
      """
        |select a.date_time,
        |       a.hour,
        |       a.time_use_sum+b.time_use_sum,
        |       (a.time_use_sum+b.time_use_sum)/(a.user_num+b.user_num) time_use_avg
        |  from live a, demand b
        | where a.date_time = b.date_time
        |   and a.hour = b.hour
      """.stripMargin)
      .map(f => f(0) + "\t" + f(1) + "\t" + 0 + "\t" + 0 + "\t" +
      0.0 + "\t" + "%.4f".format(f(2)) + "\t" + "%.4f".format(f(3)) + "\t" +
      + 0 + "\t" + 0.0 + "\t" + 0.0)
      .coalesce(1).saveAsTextFile(args(2))
  }
}
