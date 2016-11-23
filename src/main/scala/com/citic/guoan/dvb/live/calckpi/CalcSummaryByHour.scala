package com.citic.guoan.dvb.live.calckpi

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/10/17.
  */
object CalcSummaryByHour {
  case class ORIGIN_DATA(uid:String,day:String,hour:Int,time_in_use:Long,channel_name:String)
  case class CHANNEL_DICT(dict_key:String,channel_type:String)
  case class PREPARE_SUM(date:String,cover_user_num:Long)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CalcSummaryByHour")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile(args(0))
      .map(_.split("	")).filter(p => (p(3) >= args(4) && p(3) <= args(5))) //统计指定时间范围内的数据
      .map(p => ORIGIN_DATA(p(0),p(3),p(4).toInt,p(7).toLong,p(5))).toDF()
    val channelDict = sc.textFile(args(1))
      .map(_.split("	")).map ( p =>  CHANNEL_DICT(p(0),p(1))).toDF()

    //已统计数据
    val prepareSum = sc.textFile(args(2))
      .map(_.split("	")).filter(p => p(0) == "day")
      .map ( p => PREPARE_SUM(p(1),p(3).toLong)).toDF()

    //加入频道类型 - 当做原始表看待
    val od = data.join(channelDict, data("channel_name")===channelDict("dict_key"), "left")
      .select("uid","day","hour","time_in_use","channel_name","channel_type")
    od.registerTempTable("origin_data")

    //按时统计
    val groupByDate = sqlContext.sql(
      """
        select day,hour,
               count(distinct uid) as user_num,
               sum(time_in_use)/60 time_in_use
          from origin_data
         group by day,hour
      """.stripMargin
    )
    //关联上覆盖用户数
    groupByDate.join(prepareSum, groupByDate("day")===prepareSum("date"), "left")
      .select("day","hour","user_num","cover_user_num","time_in_use")
      .registerTempTable("group_by_date")

    //------------------------------------------------
    //                用户概况
    //------------------------------------------------
    val summaryResult = sqlContext.sql(
      """
        select day,
               hour,
               user_num,
               user_num*1.0/cover_user_num as cover_pct,
               time_in_use,
               time_in_use/user_num as time_in_use_avg
          from group_by_date
      """.stripMargin
    ).map(f => f(0) + "\t" + f(1) + "\t" + 1 + "\t" + f(2) + "\t" +
      "%.4f".format(f(3)) + "\t" + "%.4f".format(f(4)) + "\t" + "%.4f".format(f(5)) + "\t" +
      + 0 + "\t" + 0.0 + "\t" + 0.0)

    //output
    summaryResult.coalesce(1).saveAsTextFile(args(3)+"/summary")
  }
}
