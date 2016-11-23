package com.citic.guoan.dvb.overview

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/11/21.
  */
object SummaryByMWD {
  case class PREPARE_SUM(date:String,user_num:Long,cover_user_num:Long, user_in_num:Long,
                         user_out_num:Long,last_user_num:Long,time_in_use:Double)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SummaryByMWD")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.textFile(args(0))
      .map(_.split("	"))
      .filter(p => p(0) == args(3))
      .map ( p => PREPARE_SUM(p(1),p(2).toLong,p(3).toLong,p(4).toLong,p(5).toLong,
        p(6).toLong,p(7).toDouble)).toDF()
      .registerTempTable("live_sum")

    //点播的周要减一
    sc.textFile(args(1))
      .map(_.split("	"))
      .filter(p => p(0) == args(3))
      .map ( p => PREPARE_SUM(if(args(3) == "week") (p(1).toInt-1).toString else p(1),
        p(2).toLong,p(3).toLong,p(4).toLong,p(5).toLong,
        p(6).toLong,p(8).toDouble)).toDF()
      .registerTempTable("demand_sum")

    sqlContext.sql(
      """
        |select a.date,
        |       a.user_num+b.user_num,
        |       (a.user_num+b.user_num)*1.0/(a.cover_user_num+b.cover_user_num) as cover_pct,
        |       (a.user_num+b.user_num-a.last_user_num-b.last_user_num) * 1.0 / (a.last_user_num+b.last_user_num),
        |       a.user_in_num+b.user_in_num,
        |       a.user_out_num+b.user_out_num,
        |       a.time_in_use+b.time_in_use,
        |       (a.time_in_use+b.time_in_use)/(a.user_num+b.user_num) time_use_avg
        |  from live_sum a, demand_sum b
        | where a.date = b.date
      """.stripMargin)
      .map(f => f(0) + "\t" + 0 + "\t" + f(1) + "\t" +
      "%.4f".format(f(2)) + "\t" + "%.4f".format(f(3)) + "\t" + f(4) +"\t" + f(5) + "\t"+
      "%.4f".format(f(6)) + "\t" + "%.4f".format(f(7)) + "\t" + 0 + "\t" + 0.0 + "\t" + 0.0)
      .coalesce(1).saveAsTextFile(args(2))
  }
}
