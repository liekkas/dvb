package com.citic.guoan.dvb.demand

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/10/17.
  */
object DemandByMonth {
  case class DEMAND_DATA(uid:String,month:Int,time_in_use:Long,show_name:String)
  case class SHOW_DICT(dict_key:String,show_type:String)
  case class PREPARE_SUM(month:Int,user_num:Long,cover_user_num:Long, user_in_num:Long,
                         user_out_num:Long,last_user_num:Long,request_times:Long,
                         time_in_use:Double,all_show_num:Long)

  def main(args: Array[String]): Unit = {
    val USER_INDEX_OFFSET = args(6) //用户指数权重指数
    val FIX_TIME = 60 * 24 * 30 //该时段固定时间
    val conf = new SparkConf().setAppName("demandByMonth")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile(args(0))
      .map(_.split("	")).filter(p => (p(6) >= args(4) && p(6) <= args(5))) //统计指定时间范围内的数据
      .map(p => DEMAND_DATA(p(0),p(6).toInt,p(1).toLong,p(3))).toDF()
      .distinct()
    val showDict = sc.textFile(args(1))
      .map(_.split("	")).map ( p =>  SHOW_DICT(p(0),p(1))).toDF()

    sc.textFile(args(2))
      .map(_.split("	")).filter(p => p(0) == "month")
      .map ( p => PREPARE_SUM(p(1).toInt,p(2).toLong,p(3).toLong,p(4).toLong,p(5).toLong,
        p(6).toLong,p(7).toLong,p(8).toDouble,p(9).toLong)).toDF()
      .registerTempTable("group_by_date")
    sqlContext.cacheTable("group_by_date")

    //加入节目类型 - 当做原始表看待
    data.join(showDict, data("show_name")===showDict("dict_key"), "left")
      .select("uid","month","time_in_use","show_name","show_type")
      .registerTempTable("origin_data")
    sqlContext.cacheTable("origin_data")

    //------------------------------------------------
    //                用户概况
    //------------------------------------------------
    val summaryResult = sqlContext.sql(
      """
        select month,
               user_num,
               user_num*1.0/cover_user_num as cover_pct,
               (user_num - last_user_num) * 1.0 / last_user_num,
               user_in_num,
               user_out_num,
               time_in_use,
               time_in_use/user_num as time_in_use_avg,
               request_times,
               request_times*1.0/user_num as request_times_avg,
               time_in_use/request_times as request_one
          from group_by_date
      """.stripMargin
    )
    summaryResult.map(f => f(0) + "\t" + 2 + "\t" + f(1) + "\t" +
      "%.4f".format(f(2)) + "\t" + "%.4f".format(f(3)) + "\t" + f(4) +"\t" + f(5) + "\t"+
      "%.4f".format(f(6)) + "\t" + "%.4f".format(f(7)) + "\t" + f(8) + "\t" + "%.4f".format(f(9))+ "\t" + "%.4f".format(f(10)))
      .repartition(1).saveAsTextFile(args(3)+"/demandByMonth/summary")
    println(">>> Complete DemandByMonth::Summary!")

    //------------------------------------------------
    //                按节目类型
    //------------------------------------------------
    val groupByShowType = sqlContext.sql(
      """
        select month as show_month,
               show_type,
               count(distinct uid) as show_user_num,
               sum(time_in_use)/60 show_time_in_use,
               count(distinct show_name) as show_num
          from origin_data
         group by month,show_type
      """.stripMargin
    )
    groupByShowType.registerTempTable("group_by_show_type")

    val showTypeJoined = sqlContext.sql(
      """
        select b.month,a.show_type,a.show_user_num,a.show_time_in_use,a.show_num,
               b.user_num,b.time_in_use,b.cover_user_num,b.all_show_num
          from group_by_show_type a
     left join group_by_date b
            on a.show_month = b.month
         where a.show_type="电影" or a.show_type="电视剧"
      """.stripMargin)
    showTypeJoined.registerTempTable("group_by_show_type_result")
    showTypeJoined.show()

    val showTypeResult = sqlContext.sql(
      s"""
        select month,show_type,
               show_time_in_use/${FIX_TIME}/cover_user_num*${USER_INDEX_OFFSET} as user_index,
               show_user_num *1.0/user_num as cover_pct,
               show_time_in_use/time_in_use as market_pct,
               show_time_in_use/show_user_num as time_in_use_avg,
               show_time_in_use,
               show_user_num,
               show_num*1.0/all_show_num as show_num_pct,
               time_in_use,
               user_num
          from group_by_show_type_result
      """.stripMargin)
    showTypeResult.map(f => f(0) + "\t" + f(1) + "\t" +
      "%.4f".format(f(2)) + "\t" + "%.4f".format(f(3)) + "\t" + "%.4f".format(f(4)) + "\t" +
      "%.4f".format(f(5)) + "\t" + "%.4f".format(f(6)) + "\t" + f(7) + "\t" +
      "%.4f".format(f(8)) + "\t" + "%.4f".format(f(9)) + "\t" + f(10)
    ).repartition(1).saveAsTextFile(args(3)+"/demandByMonth/showType")
    println(">>> Complete DemandByMonth::ShowType!")

    //------------------------------------------------
    //                按具体节目
    //------------------------------------------------
    val groupByShow = sqlContext.sql(
      """
        select month as show_month,
               show_type,
               show_name,
               count(distinct uid) as show_user_num,
               sum(time_in_use)/60 show_time_in_use
          from origin_data
         group by month,show_type,show_name
      """.stripMargin
    )
    groupByShow.registerTempTable("group_by_show")

    val showJoined = sqlContext.sql(
      """
        select b.month,a.show_type,a.show_name,a.show_user_num,a.show_time_in_use,
               b.user_num,b.time_in_use,b.cover_user_num
          from group_by_show a
     left join group_by_date b
            on a.show_month = b.month
         where a.show_type="电影" or a.show_type="电视剧"
      """.stripMargin)
    showJoined.registerTempTable("group_by_show_result")
    showJoined.show()

    val showResult = sqlContext.sql(
      s"""
        select month,show_name,show_type,
               show_time_in_use/${FIX_TIME}/cover_user_num*${USER_INDEX_OFFSET} as user_index,
               show_user_num*1.0/user_num as cover_pct,
               show_time_in_use/time_in_use as market_pct,
               show_time_in_use/show_user_num as time_in_use_avg,
               show_time_in_use,
               show_user_num,
               time_in_use,
               user_num
          from group_by_show_result
      """.stripMargin)
    showResult.map(f => f(0) + "\t" + f(1) + "\t" + f(2)+ "\t" +
      "%.4f".format(f(3)) + "\t" + "%.4f".format(f(4)) + "\t" + "%.4f".format(f(5)) + "\t" +
      "%.4f".format(f(6)) + "\t" + "%.4f".format(f(7)) + "\t" + f(8) + "\t" + "%.4f".format(f(9)) + "\t" + f(10)
    ).repartition(1).saveAsTextFile(args(3)+"/demandByMonth/show")
    println(">>> Complete DemandByMonth::Show!")
  }
}
