package com.citic.guoan.dvb

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/10/17.
  */
object DemandByHour {
  case class DEMAND_DATA(uid:String,day:String,hour:Int,remain_time:Long,channel_name:String)
  case class SHOW_TYPE(show_name:String,show_type:String)
  case class UID_COUNT(date_type:String,date:String,cover_user_num:Long)

  def main(args: Array[String]): Unit = {
    val USER_INDEX_OFFSET = 1000
    val FIX_TIME = 60
    val conf = new SparkConf().setMaster("local").setAppName("demandByHour")
    //    val conf = new SparkConf().setAppName("demandByHour")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile(args(0))
      .map(_.split("	")).filter(p => (p(2) >= args(4) && p(2) <= args(5))) //过滤掉用不着的数据
      .map(p => DEMAND_DATA(p(0),p(2),p(4).toInt,p(1).toLong,p(3))).toDF()
    val showDict = sc.textFile(args(1))
      .map(_.split("	")).map ( p =>  SHOW_TYPE(p(0),p(1))).toDF()
    val uidCount = sc.textFile(args(2))
      .map(_.split("	")).filter(p => p(0).equals("day"))
      .map ( p =>  UID_COUNT(p(0),p(1),p(3).toLong)).toDF()

    //加入节目类型 - 当做原始表看待
    val demandData = data.join(showDict, data("channel_name")===showDict("show_name"), "left")
      .select("uid","day","hour","remain_time","channel_name","show_type")
    demandData.registerTempTable("demand_origin")
    sqlContext.cacheTable("demand_origin")

    //按时统计
    val hourSum = sqlContext.sql(
      """
        select day,hour,
          count(distinct uid) as user_num,
          sum(remain_time)/60 remain_time,
          count(*) request_times,
          count(distinct channel_name) all_show_num
        from demand_origin group by day,hour
      """.stripMargin
    )
    val summary = hourSum.join(uidCount, hourSum("day")===uidCount("date"), "left")
      .select("day","hour","user_num","cover_user_num","remain_time","request_times","all_show_num")
    summary.registerTempTable("demand_hour_sum")
    sqlContext.cacheTable("demand_hour_sum")

    //------------------------------------------------
    //                用户概况
    //------------------------------------------------
    val summaryResult = sqlContext.sql(
      """
        select
          day,
          hour,
          user_num,
          user_num*1.0/cover_user_num as coverPct,
          remain_time,
          remain_time/user_num as timeUseAVG,
          request_times,
          request_times*1.0/user_num as requestAVG,
          remain_time/request_times as requestOne
        from demand_hour_sum
      """.stripMargin
    )
    summaryResult.map(f => f(0) + "\t" + f(1) + "\t" + 2 + "\t" + f(2) + "\t" + "%.4f".format(f(3)) + "\t" +
      "%.4f".format(f(4)) + "\t" + "%.4f".format(f(5)) + "\t" + f(6) + "\t" + "%.4f".format(f(7))+ "\t" + "%.4f".format(f(8)))
        .repartition(1).saveAsTextFile(args(3)+"/demandByHour/summary")

    println(">>> Complete Summary!")

    //------------------------------------------------
    //                按节目类型
    //------------------------------------------------
    val showType = sqlContext.sql(
      """
        select day as show_day,hour as show_hour,show_type,
          count(distinct uid) as show_user_num,
          sum(remain_time)/60 show_remain_time,
          count(distinct channel_name) as show_num
        from demand_origin
        group by day,hour,show_type
      """.stripMargin
    )
    showType.registerTempTable("demand_show_type_sum")

    val showTypeJoined = sqlContext.sql(
      """
        select b.day,b.hour,a.show_type,a.show_user_num,a.show_remain_time,show_num,
               b.user_num,b.remain_time,b.cover_user_num,b.all_show_num
          from demand_show_type_sum a
     left join demand_hour_sum b
            on a.show_day = b.day and a.show_hour = b.hour
         where a.show_type="电影" or a.show_type="电视剧"
      """.stripMargin)
    showTypeJoined.registerTempTable("demand_show_type")
    showTypeJoined.show()

    val showTypeResult = sqlContext.sql(
      s"""
        select day,hour,show_type,
                show_remain_time / ${FIX_TIME} / cover_user_num * ${USER_INDEX_OFFSET} as userIndex,
                show_user_num * 1.0 / user_num as coverPct,
                show_remain_time / remain_time as marketPct,
                show_remain_time / show_user_num as timeUseAVG,
                show_remain_time,
                show_user_num,
                show_num * 1.0 / all_show_num as showRatio,
                remain_time,
                user_num
          from demand_show_type
      """.stripMargin)
    showTypeResult.map(f => f(0) + "\t" + f(1) + "\t" + f(2) + "\t" + "%.4f".format(f(3)) + "\t" +
      "%.4f".format(f(4)) + "\t" + "%.4f".format(f(5)) + "\t" +
      "%.4f".format(f(6)) + "\t" + "%.4f".format(f(7)) + "\t" + f(8) + "\t" +
      "%.4f".format(f(9)) + "\t" + "%.4f".format(f(10)) + "\t" + f(11)
    ).repartition(1).saveAsTextFile(args(3)+"/demandByHour/showType")
    println(">>> Complete DemandByHourShowType!")

    //------------------------------------------------
    //                按具体节目
    //------------------------------------------------
    val show = sqlContext.sql(
      """
        select day as show_day,hour as show_hour,show_type,channel_name as show_name,
          count(distinct uid) as show_user_num,
          sum(remain_time)/60 show_remain_time
        from demand_origin
        group by day,hour,show_type,channel_name
      """.stripMargin
    )
    show.registerTempTable("demand_show_sum")

    val showJoined = sqlContext.sql(
      """
        select b.day,b.hour,a.show_type,a.show_name,a.show_user_num,a.show_remain_time,
               b.user_num,b.remain_time,b.cover_user_num
          from demand_show_sum a
     left join demand_hour_sum b
            on a.show_day = b.day and a.show_hour = b.hour
         where a.show_type="电影" or a.show_type="电视剧"
      """.stripMargin)
    showJoined.registerTempTable("demand_show")
    showJoined.show()

    val showResult = sqlContext.sql(
      s"""
        select day,hour,show_name,show_type,
                show_remain_time / ${FIX_TIME} / cover_user_num * ${USER_INDEX_OFFSET} as userIndex,
                show_user_num * 1.0 / user_num as coverPct,
                show_remain_time / remain_time as marketPct,
                show_remain_time / show_user_num as timeUseAVG,
                show_remain_time,
                show_user_num,
                remain_time,
                user_num
          from demand_show
      """.stripMargin)
    showResult.map(f => f(0) + "\t" + f(1) + "\t" + f(2)+ "\t" + f(3) + "\t" +
      "%.4f".format(f(4)) + "\t" + "%.4f".format(f(5)) + "\t" + "%.4f".format(f(6)) + "\t" +
      "%.4f".format(f(7)) + "\t" + "%.4f".format(f(8)) + "\t" + f(9) + "\t" + "%.4f".format(f(10)) + "\t" + f(11)
    ).repartition(1).saveAsTextFile(args(3)+"/demandByHour/show")
    println(">>> Complete DemandByHourShow!")
  }
}
