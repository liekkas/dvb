package com.citic.guoan.dvb.live

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/10/17.
  */
object LiveByDay {
  case class ORIGIN_DATA(uid:String,day:String,time_in_use:Long,channel_name:String)
  case class CHANNEL_DICT(dict_key:String,channel_type:String)
  case class PREPARE_SUM(day:String,user_num:Long,cover_user_num:Long, user_in_num:Long,
                         user_out_num:Long,last_user_num:Long,time_in_use:Double)

  def main(args: Array[String]): Unit = {
    val USER_INDEX_OFFSET = args(6) //用户指数权重指数
    val FIX_TIME = 60 * 24 //该时段固定时间
    val conf = new SparkConf().setAppName("LiveByDay")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile(args(0))
      .map(_.split("	")).filter(p => (p(3) >= args(4) && p(3) <= args(5))) //统计指定时间范围内的数据
      .map(p => ORIGIN_DATA(p(0),p(3),p(7).toLong,p(5))).toDF()
    val channelDict = sc.textFile(args(1))
      .map(_.split("	")).map ( p =>  CHANNEL_DICT(p(0),p(1))).toDF()

    //已统计数据
    sc.textFile(args(2))
      .map(_.split("	")).filter(p => p(0) == "day")
      .map ( p => PREPARE_SUM(p(1),p(2).toLong,p(3).toLong,p(4).toLong,p(5).toLong,
        p(6).toLong,p(7).toDouble)).toDF()
      .registerTempTable("group_by_date")

    //加入频道类型 - 当做原始表看待
    val od = data.join(channelDict, data("channel_name")===channelDict("dict_key"), "left")
      .select("uid","day","time_in_use","channel_name","channel_type")
//    od.persist(StorageLevel.MEMORY_AND_DISK_SER)
    od.registerTempTable("origin_data")
    sqlContext.cacheTable("origin_data")

    //------------------------------------------------
    //                用户概况
    //------------------------------------------------
    val summaryResult = sqlContext.sql(
      """
        select day,
               user_num,
               user_num*1.0/cover_user_num as cover_pct,
               (user_num - last_user_num) * 1.0 / last_user_num,
               user_in_num,
               user_out_num,
               time_in_use,
               time_in_use/user_num as time_in_use_avg
          from group_by_date
      """.stripMargin
    ).map(f => f(0) + "\t" + 1 + "\t" + f(1) + "\t" +
      "%.4f".format(f(2)) + "\t" + "%.4f".format(f(3)) + "\t" + f(4) +"\t" + f(5) + "\t"+
      "%.4f".format(f(6)) + "\t" + "%.4f".format(f(7)) + "\t" + 0 + "\t" + 0.0 + "\t" + 0.0)
    

    //------------------------------------------------
    //                按频道类型
    //------------------------------------------------
    val groupByChannelType = sqlContext.sql(
      """
        select day as channel_day,
               channel_type,
               count(distinct uid) as channel_user_num,
               sum(time_in_use)/60 channel_time_in_use
          from origin_data
         where channel_type="央视频道" or channel_type="卫视频道"
         group by day,channel_type
      """.stripMargin
    )
    groupByChannelType.registerTempTable("group_by_channel_type")

    val channelTypeJoined = sqlContext.sql(
      """
        select b.day,a.channel_type,a.channel_user_num,a.channel_time_in_use,
               b.user_num,b.time_in_use,b.cover_user_num
          from group_by_channel_type a
     left join group_by_date b
            on a.channel_day = b.day
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
               user_num
          from group_by_channel_type_result
      """.stripMargin)
    .map(f => f(0) + "\t" + f(1) + "\t" +
      "%.4f".format(f(2)) + "\t" + "%.4f".format(f(3)) + "\t" + "%.4f".format(f(4)) + "\t" +
      "%.4f".format(f(5)) + "\t" + "%.4f".format(f(6)) + "\t" + f(7) + "\t" +
      "%.4f".format(f(8)) + "\t" + f(9)
    )

    //------------------------------------------------
    //                按具体频道
    //------------------------------------------------
    val groupByShow = sqlContext.sql(
      """
        select day as channel_day,
               channel_type,
               channel_name,
               count(distinct uid) as channel_user_num,
               sum(time_in_use)/60 channel_time_in_use
          from origin_data
         where channel_type="央视频道" or channel_type="卫视频道"
         group by day,channel_type,channel_name
      """.stripMargin
    )
    groupByShow.registerTempTable("group_by_channel")

    val showJoined = sqlContext.sql(
      """
        select b.day,a.channel_type,a.channel_name,a.channel_user_num,a.channel_time_in_use,
               b.user_num,b.time_in_use,b.cover_user_num
          from group_by_channel a
     left join group_by_date b
            on a.channel_day = b.day
      """.stripMargin)
    showJoined.registerTempTable("group_by_channel_result")

    val showResult = sqlContext.sql(
      s"""
        select day,channel_name,channel_type,
               channel_time_in_use/${FIX_TIME}/cover_user_num*${USER_INDEX_OFFSET} as user_index,
               channel_time_in_use/time_in_use as market_pct,
               channel_user_num*1.0/user_num as cover_pct,
               channel_time_in_use/channel_user_num as time_in_use_avg,
               channel_time_in_use,
               channel_user_num,
               time_in_use,
               user_num
          from group_by_channel_result
      """.stripMargin)
    .map(f => f(0) + "\t" + f(1) + "\t" + f(2)+ "\t" +
      "%.4f".format(f(3)) + "\t" + "%.4f".format(f(4)) + "\t" + "%.4f".format(f(5)) + "\t" +
      "%.4f".format(f(6)) + "\t" + "%.4f".format(f(7)) + "\t" + f(8) + "\t" + "%.4f".format(f(9)) + "\t" + f(10)
    )

    //output
    summaryResult.coalesce(1).saveAsTextFile(args(3)+"/summary")
    channelTypeResult.coalesce(1).saveAsTextFile(args(3)+"/channelType")
    showResult.coalesce(1).saveAsTextFile(args(3)+"/channel")
  }
}
