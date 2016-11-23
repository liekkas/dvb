package com.citic.guoan.dvb.live.calckpi

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/10/17.
  */
object CalcChannelTypeByHour {
  case class ORIGIN_DATA(uid:String,day:String,hour:Int,time_in_use:Long,channel_name:String)
  case class CHANNEL_DICT(dict_key:String,channel_type:String)
  case class PREPARE_SUM(date:String,cover_user_num:Long)

  def main(args: Array[String]): Unit = {
    val USER_INDEX_OFFSET = args(6) //用户指数权重指数
    val FIX_TIME = 60 //该时段固定时间
    val conf = new SparkConf().setAppName("CalcChannelTypeByHour")
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
//    sqlContext.cacheTable("origin_data")

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
    //                按频道类型
    //------------------------------------------------
    val groupByChannelType = sqlContext.sql(
      """
        select day as channel_day,
               hour as channel_hour,
               channel_type,
               count(distinct uid) as channel_user_num,
               sum(time_in_use)/60 channel_time_in_use
          from origin_data
         where channel_type="央视频道" or channel_type="卫视频道"
         group by day,hour,channel_type
      """.stripMargin
    )
    groupByChannelType.registerTempTable("group_by_channel_type")

    val channelTypeJoined = sqlContext.sql(
      """
        select b.day,b.hour,a.channel_type,a.channel_user_num,a.channel_time_in_use,
               b.user_num,b.time_in_use,b.cover_user_num
          from group_by_channel_type a
     left join group_by_date b
            on a.channel_day = b.day and a.channel_hour = b.hour
      """.stripMargin)
    channelTypeJoined.registerTempTable("group_by_channel_type_result")

    val channelTypeResult = sqlContext.sql(
      s"""
        select day,channel_type,
               channel_time_in_use/${FIX_TIME}/cover_user_num*${USER_INDEX_OFFSET} as user_index,
               channel_time_in_use/time_in_use as market_pct,
               channel_user_num *1.0/user_num as cover_pct,
               channel_time_in_use/channel_user_num as time_in_use_avg,
               channel_time_in_use,
               channel_user_num,
               time_in_use,
               user_num,
               hour
          from group_by_channel_type_result
      """.stripMargin)
    .map(f => f(0) + "\t" + f(10) + "\t" + f(1) + "\t" +
      "%.4f".format(f(2)) + "\t" + "%.4f".format(f(3)) + "\t" + "%.4f".format(f(4)) + "\t" +
      "%.4f".format(f(5)) + "\t" + "%.4f".format(f(6)) + "\t" + f(7) + "\t" +
      "%.4f".format(f(8)) + "\t" + f(9)
    )

    channelTypeResult.coalesce(1).saveAsTextFile(args(3)+"/channelType")
  }
}
