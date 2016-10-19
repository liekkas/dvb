package com.citic.guoan.dvb

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Created by liekkas on 16/10/17.
  */
object DemandByWeek {
  case class DEMAND_DATA(uid:String,week:Int,remain_time:Long,channel_name:String)
  case class SHOW_TYPE(show_name:String,show_type:String)

  def main(args: Array[String]): Unit = {
    val USER_INDEX_OFFSET = 1000
    //计算中间结果先放到redis中,最后一并导出文本
    val jedis = new Jedis("localhost")
    val conf = new SparkConf().setMaster("local").setAppName("demandByWeek")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile(args(0))
      .map(_.split("	")).filter(p => p(9).toInt > 201612) //过滤掉用不着的数据
      .map(p => DEMAND_DATA(p(0),p(9).toInt,p(3).toLong,p(6))).toDF()
    data.registerTempTable("demand_origin_temp")
    data.show()

    val showDict = sc.textFile(args(1))
      .map(_.split("	")).map ( p =>  SHOW_TYPE(p(0),p(1))).toDF()
    showDict.registerTempTable("show_dict")

    //加入节目类型 -- 这块比较耗时,如果原始数据能提供更好
    data.join(showDict, data("channel_name")===showDict("show_name"), "left")
        .select("uid","week","remain_time","channel_name","show_type")
        .registerTempTable("demand_origin")

    //按周统计
    sqlContext.sql(
      """
        select week,sum(remain_time)/60 remain_time,count(*) request_times,count(distinct uid) as usernum
        from demand_origin group by week
      """.stripMargin
    ).registerTempTable("demand_week_sum")

    //按节目类型
    sqlContext.sql(
      """
        select week,show_type,count(distinct uid) as usernum,sum(remain_time)/60 remain_time,count(distinct channel_name) as shownum
        from demand_origin group by week,show_type
      """.stripMargin
    ).registerTempTable("demand_show_type")

    //按节目
    sqlContext.sql(
      """
        select week,show_type,channel_name,count(distinct uid) as usernum,sum(remain_time)/60 remain_time
        from demand_origin group by week,show_type,channel_name
      """.stripMargin
    ).registerTempTable("demand_channel")


    //本次需要计算的自然周
    val weeks = 201614 to 201640
    weeks.foreach(week => {
      println(">>> Process Demand Week:" + week)
      val lastWeek = week - 1

      val sumDF = sqlContext.sql("select * from demand_week_sum where week=" + week)
      val lastUserNumDF = sqlContext.sql("select * from demand_week_sum where week=" + lastWeek)
      val size = sumDF.count()
      val lastSize = lastUserNumDF.count()

      println(">>> Week Size:"+size+" LastWeek Size:"+lastSize)
      if(size > 0 && lastSize > 0) {
        val lastUserNum = lastUserNumDF.first().getLong(3)
        val sum = sumDF.first()
        val remainTime = sum.getDouble(1)
        val requestTimes = sum.getLong(2)
        val userNum = sum.getLong(3)

        //      //用户概况和行为
        val coverUserNum = sqlContext.sql("select count(distinct(uid)) from demand_origin where week<=" + week)
          .first().getLong(0)

        //流入用户数 - 昨天没在线今天在线
        val userInNum = sqlContext.sql("select count(b.uid) from " +
          "(SELECT distinct(uid) uid FROM demand_origin where week=" + lastWeek + ") as a " +
          "right join (SELECT distinct(uid) uid FROM demand_origin where week=" + week + ") as b " +
          "on a.uid = b.uid where a.uid is null").first().getLong(0)
        //流出用户数 - 昨天在线今天没在线
        val userOutNum = sqlContext.sql("select count(a.uid) from " +
          "(SELECT distinct(uid) uid FROM demand_origin where week=" + lastWeek + ") as a " +
          "left join (SELECT distinct(uid) uid FROM demand_origin where week=" + week + ") as b " +
          "on a.uid = b.uid where b.uid is null").first().getLong(0)

        val coverPct = userNum * 1.0 / coverUserNum
        val userIncreasePct = (userNum - lastUserNum) * 1.0 / lastUserNum
        val timeUseAVG = remainTime / userNum
        val requestAVG = requestTimes * 1.0 / userNum
        val requestOne = remainTime / requestTimes

        val summary = week + "\t" + 2 + "\t" + userNum + "\t" + coverPct + "\t" + userIncreasePct + "\t" + userInNum + "\t" +
          userOutNum + "\t" + remainTime + "\t" + timeUseAVG + "\t" + requestTimes + "\t" + requestAVG + "\t" + requestOne

        jedis.sadd("SUMMARY_W", summary)
        println(">>> Complete SummaryW:"+summary)

        //点播频道类型
        val allShow = sqlContext.sql("select sum(shownum) " +
          "from demand_show_type t where t.week=" + week).first()
        val allShowNum = allShow.getLong(0) //所有的节目数量
        val FIX_TIME = 60 * 24 * 30

        //电影类
        val movieDF = sqlContext.sql("select shownum, remain_time, usernum " +
          "from demand_show_type t where t.week=" + week + " and t.show_type='电影'")
        if(movieDF.count() > 0) {
          val movie = movieDF.first()
          val movieNum = movie.getLong(0) //节目数量
          val movieRemainTime = movie.getDouble(1) //点播时间
          val movieUserNum = movie.getLong(2) //观看用户数
          val movieMarketPct = movieRemainTime / remainTime
          val movieCoverPct = movieUserNum * 1.0 / userNum
          val movieUserIndex = movieRemainTime / FIX_TIME / coverUserNum * USER_INDEX_OFFSET
          val movieTimeUseAVG = movieRemainTime / movieUserNum
          val movieShowRatio = movieNum * 1.0 / allShowNum

          val demandMovie = week + "\t" + "电影" + "\t" + movieUserIndex + "\t" + movieCoverPct + "\t" + movieMarketPct + "\t" +
            movieTimeUseAVG + "\t" + movieRemainTime + "\t" + movieUserNum + "\t" + movieShowRatio + "\t" + remainTime + "\t" + userNum
          jedis.sadd("DEMAND_W", demandMovie)
          println(">>> Complete demandW_movie:" + demandMovie)
        }

        //电视剧类
        val tvDF = sqlContext.sql("select shownum, remain_time, usernum " +
          "from demand_show_type t where t.week=" + week + " and t.show_type='电视剧'")
        if(tvDF.count() > 0) {
          val tv = tvDF.first()
          val tvNum = tv.getLong(0) //节目数量
          val tvRemainTime = tv.getDouble(1) //点播时间
          val tvUserNum = tv.getLong(2) //观看用户数
          val tvMarketPct = tvRemainTime / remainTime
          val tvCoverPct = tvUserNum * 1.0 / userNum
          val tvUserIndex = tvRemainTime / FIX_TIME / coverUserNum * USER_INDEX_OFFSET
          val tvTimeUseAVG = tvRemainTime / tvUserNum
          val tvShowRatio = tvNum * 1.0 / allShowNum

          val demandTv = week + "\t" + "电视剧" + "\t" + tvUserIndex + "\t" + tvCoverPct + "\t" + tvMarketPct + "\t" +
            tvTimeUseAVG + "\t" + tvRemainTime + "\t" + tvUserNum + "\t" + tvShowRatio + "\t" + remainTime + "\t" + userNum
          jedis.sadd("DEMAND_W", demandTv)
          println(">>> Complete demandW_tv:" + demandTv)
        }

        //点播节目
        val shows = sqlContext.sql("select channel_name,remain_time, show_type, usernum" +
          " from demand_channel where show_type = '电视剧' or show_type = '电影' and week=" + week).collect()
        if(shows.size > 0) {
          shows.foreach(show => {
            val showName = show.getString(0)
            val showRemainTime = show.getDouble(1)
            val showType = show.getString(2)
            val showUserNum = show.getLong(3)
            val showMarketPct = showRemainTime / remainTime
            val showCoverPct = showUserNum * 1.0 / userNum
            val showUserIndex = showRemainTime / FIX_TIME / coverUserNum * USER_INDEX_OFFSET
            val showTimeUseAVG = showRemainTime / showUserNum

            val demandShow = week+"\t"+showName+"\t"+showType+"\t"+showUserIndex+"\t"+showCoverPct+"\t"+showMarketPct+"\t" +
              showTimeUseAVG+"\t"+showRemainTime+"\t"+showUserNum+"\t"+remainTime+"\t"+userNum
            jedis.sadd("DEMAND_SHOWS_W", demandShow)
          })
        }
      }
    })
  }
}
