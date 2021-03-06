package com.citic.guoan.dvb.demand

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by liekkas on 16/10/17.
  */
object DemandByHour {
  case class DEMAND_DATA(uid:String,day:String,hour:Int,time_in_use:Long,show_name:String,flag:String)
  case class SHOW_DICT(dict_key:String,show_type:String)
  case class UID_COUNT(date_type:String,date:String,cover_user_num:Long)

  def main(args: Array[String]): Unit = {
    val USER_INDEX_OFFSET = args(6) //用户指数权重指数
    val FIX_TIME = 60 //该时段固定时间
    val conf = new SparkConf().setAppName("demandByHour")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile(args(0))
      .map(_.split("	")).filter(p => (p(2) >= args(4) && p(2) <= args(5))) //统计指定时间范围内的数据
      .map(p => DEMAND_DATA(p(0),p(2),p(4).toInt,p(1).toLong,p(3),p(7))).toDF()
      .distinct()

    val showDict = sc.textFile(args(1))
      .map(_.split("	")).map ( p =>  SHOW_DICT(p(0),p(1))).toDF()
    val uidCount = sc.textFile(args(2))
      .map(_.split("	")).filter(p => p(0) == "day")
      .map ( p =>  UID_COUNT(p(0),p(1),p(3).toLong)).toDF()

    //加入节目类型 - 当做原始表看待
    data.join(showDict, data("show_name")===showDict("dict_key"), "left")
      .select("uid","day","hour","time_in_use","show_name","show_type","flag")
      .registerTempTable("origin_data")
    sqlContext.cacheTable("origin_data")

    //按时统计
    val groupByDate = sqlContext.sql(
      """
        select day,hour,
               count(distinct uid) as user_num,
               sum(time_in_use)/60 time_in_use,
               count(distinct show_name) all_show_num
          from origin_data
         group by day,hour
      """.stripMargin
    )
    //统计点播次数 - 和上面不同,需要判断flag,所以单独拿出来计算
    val requestTimes = sqlContext.sql(
      """
        select day as rt_day,hour as rt_hour, count(*) request_times
          from origin_data
         where flag='true'
         group by day,hour
      """.stripMargin
    )
    //关联上覆盖用户数
    val coverUserNum = groupByDate.join(uidCount, groupByDate("day")===uidCount("date"), "left")
      .select("day","hour","user_num","cover_user_num","time_in_use","all_show_num")
    //关联上点播次数
    coverUserNum.join(requestTimes,
      coverUserNum("day")===requestTimes("rt_day") && coverUserNum("hour")===requestTimes("rt_hour"), "left")
        .select("day","hour","user_num","cover_user_num","time_in_use","all_show_num","request_times")
      .registerTempTable("group_by_date")
    sqlContext.cacheTable("group_by_date")
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
               time_in_use/user_num as time_in_use_avg,
               request_times,
               request_times*1.0/user_num as request_times_avg,
               time_in_use/request_times as request_one
          from group_by_date
      """.stripMargin
    )
    summaryResult.map(f => {
      val requestOne = if(f(8) != null) "%.4f".format(f(8)).toFloat else 0
      val rone = if(requestOne > 60) 60 else requestOne
      f(0) + "\t" + f(1) + "\t" + 2 + "\t" + f(2) + "\t" +
        "%.4f".format(f(3)) + "\t" + "%.4f".format(f(4)) + "\t" + "%.4f".format(f(5)) + "\t" +
        f(6) + "\t" + "%.4f".format(f(7))+ "\t" + rone
    })
        .repartition(1).saveAsTextFile(args(3)+"/demandByHour/summary")
    println(">>> Complete DemandByHour::Summary!")

    //------------------------------------------------
    //                按节目类型
    //------------------------------------------------
    val groupByShowType = sqlContext.sql(
      """
        select day as show_day,
               hour as show_hour,
               show_type,
               count(distinct uid) as show_user_num,
               sum(time_in_use)/60 show_time_in_use,
               count(distinct show_name) as show_num
          from origin_data
         group by day,hour,show_type
      """.stripMargin
    )
    groupByShowType.registerTempTable("group_by_show_type")

    val showTypeJoined = sqlContext.sql(
      """
        select b.day,b.hour,a.show_type,a.show_user_num,a.show_time_in_use,a.show_num,
               b.user_num,b.time_in_use,b.cover_user_num,b.all_show_num
          from group_by_show_type a
     left join group_by_date b
            on a.show_day = b.day and a.show_hour = b.hour
         where a.show_type="电影" or a.show_type="电视剧"
      """.stripMargin)
    showTypeJoined.registerTempTable("group_by_show_type_result")
    showTypeJoined.show()

    val showTypeResult = sqlContext.sql(
      s"""
        select day,hour,show_type,
               show_time_in_use/${FIX_TIME}/cover_user_num*${USER_INDEX_OFFSET} as user_index,
               show_user_num *1.0/cover_user_num as cover_pct,
               show_time_in_use/time_in_use as market_pct,
               show_time_in_use/show_user_num as time_in_use_avg,
               show_time_in_use,
               show_user_num,
               show_num*1.0/all_show_num as show_num_pct,
               time_in_use,
               user_num
          from group_by_show_type_result
      """.stripMargin)
    showTypeResult.map(f => f(0) + "\t" + f(1) + "\t" + f(2) + "\t" +
      "%.4f".format(f(3)) + "\t" + "%.4f".format(f(4)) + "\t" + "%.4f".format(f(5)) + "\t" +
      "%.4f".format(f(6)) + "\t" + "%.4f".format(f(7)) + "\t" + f(8) + "\t" +
      "%.4f".format(f(9)) + "\t" + "%.4f".format(f(10)) + "\t" + f(11)
    ).repartition(1).saveAsTextFile(args(3)+"/demandByHour/showType")
    println(">>> Complete DemandByHour::ShowType!")

    //------------------------------------------------
    //                按具体节目
    //------------------------------------------------
    val groupByShow = sqlContext.sql(
      """
        select day as show_day,
               hour as show_hour,
               show_type,
               show_name,
               count(distinct uid) as show_user_num,
               sum(time_in_use)/60 show_time_in_use
          from origin_data
         group by day,hour,show_type,show_name
      """.stripMargin
    )
    groupByShow.registerTempTable("group_by_show")

    val showJoined = sqlContext.sql(
      """
        select b.day,b.hour,a.show_type,a.show_name,a.show_user_num,a.show_time_in_use,
               b.user_num,b.time_in_use,b.cover_user_num
          from group_by_show a
     left join group_by_date b
            on a.show_day = b.day and a.show_hour = b.hour
         where a.show_type="电影" or a.show_type="电视剧"
      """.stripMargin)
    showJoined.registerTempTable("group_by_show_result")
    showJoined.show()

    val showResult = sqlContext.sql(
      s"""
        select day,hour,show_name,show_type,
               show_time_in_use/${FIX_TIME}/cover_user_num*${USER_INDEX_OFFSET} as user_index,
               show_user_num*1.0/cover_user_num as cover_pct,
               show_time_in_use/time_in_use as market_pct,
               show_time_in_use/show_user_num as time_in_use_avg,
               show_time_in_use,
               show_user_num,
               time_in_use,
               user_num
          from group_by_show_result
      """.stripMargin)
    showResult.map(f => f(0) + "\t" + f(1) + "\t" + f(2)+ "\t" + f(3) + "\t" +
      "%.4f".format(f(4)) + "\t" + "%.4f".format(f(5)) + "\t" + "%.4f".format(f(6)) + "\t" +
      "%.4f".format(f(7)) + "\t" + "%.4f".format(f(8)) + "\t" + f(9) + "\t" + "%.4f".format(f(10)) + "\t" + f(11)
    ).repartition(1).saveAsTextFile(args(3)+"/demandByHour/show")
    println(">>> Complete DemandByHour::Show!")
  }
}
