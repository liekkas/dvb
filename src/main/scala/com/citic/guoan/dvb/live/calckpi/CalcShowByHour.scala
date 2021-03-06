package com.citic.guoan.dvb.live.calckpi

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/10/17.
  */
object CalcShowByHour {
  case class ORIGIN_DATA(uid:String,day:String,hour:Int,time_in_use:Long,channel_name:String,show_name:String)
  case class CHANNEL_DICT(dict_key:String,channel_type:String)
  case class PREPARE_SUM(date:String,cover_user_num:Long)

  def main(args: Array[String]): Unit = {
    val USER_INDEX_OFFSET = args(6) //用户指数权重指数
    val FIX_TIME = 60 //该时段固定时间
    val conf = new SparkConf().setAppName("CalcShowByHour")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile(args(0))
      .filter(_ != "")
      .map(_.split("\t")).filter(p => (p(3) >= args(4) && p(3) <= args(5))) //统计指定时间范围内的数据
      .map(p => ORIGIN_DATA(p(0),p(3),p(4).toInt,p(7).toLong,p(5),p(6))).toDF()
    val channelDict = sc.textFile(args(1))
      .map(_.split("\t")).map ( p =>  CHANNEL_DICT(p(0),p(1))).toDF()

    //已统计数据
    val prepareSum = sc.textFile(args(2))
      .map(_.split("\t")).filter(p => p(0) == "day")
      .map ( p => PREPARE_SUM(p(1),p(3).toLong)).toDF()

    //加入频道类型 - 当做原始表看待
    val od = data.join(channelDict, data("channel_name")===channelDict("dict_key"), "left")
      .select("uid","day","hour","time_in_use","channel_name","channel_type","show_name")
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
    //                按具体节目
    //------------------------------------------------
    val groupByShow = sqlContext.sql(
      """
        select day as channel_day,
               hour as channel_hour,
               channel_type,
               channel_name,
               count(distinct uid) as show_user_num,
               sum(time_in_use)/60 show_time_in_use,
               show_name
          from origin_data
         where channel_type="央视频道" or channel_type="卫视频道"
         group by day,hour,channel_type,channel_name,show_name
      """.stripMargin
    )
    groupByShow.registerTempTable("group_by_show")

    val showJoined = sqlContext.sql(
      """
        select b.day,b.hour,a.channel_type,a.channel_name,a.show_user_num,a.show_time_in_use,
               b.user_num,b.time_in_use,b.cover_user_num,a.show_name
          from group_by_show a
     left join group_by_date b
            on a.channel_day = b.day and a.channel_hour = b.hour
      """.stripMargin)
    showJoined.registerTempTable("group_by_show_result")

    val showResult = sqlContext.sql(
      s"""
        select day,channel_name,channel_type,
               show_time_in_use/${FIX_TIME}/cover_user_num*${USER_INDEX_OFFSET} as user_index,
               show_time_in_use/time_in_use as market_pct,
               show_user_num*1.0/cover_user_num as cover_pct,
               show_time_in_use/show_user_num as time_in_use_avg,
               show_time_in_use,
               show_user_num,
               time_in_use,
               user_num,
               hour,
               show_name
          from group_by_show_result
         order by user_index desc
      """.stripMargin)
    .map(f => f(0) + "\t" + f(11) + "\t" + f(12) + "\t" + f(1) + "\t" + f(2)+ "\t" +
      "%.4f".format(f(3)) + "\t" + "%.4f".format(f(4)) + "\t" + "%.4f".format(f(5)) + "\t" +
      "%.4f".format(f(6)) + "\t" + "%.4f".format(f(7)) + "\t" + f(8) + "\t" + "%.4f".format(f(9)) + "\t" + f(10)
    )

    showResult.repartition(1).saveAsTextFile(args(3)+"/show", classOf[GzipCodec])
  }
}
