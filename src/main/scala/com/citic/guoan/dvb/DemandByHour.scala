package com.citic.guoan.dvb

import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Created by liekkas on 16/10/17.
  */
object DemandByHour {
  case class DEMAND_DATA(uid:String,day:String,hour:Int,remain_time:Long,channel_name:String)
  case class SHOW_TYPE(show_name:String,show_type:String)

  def main(args: Array[String]): Unit = {
    val USER_INDEX_OFFSET = 10000
    //计算中间结果先放到redis中,最后一并导出文本
    val jedis = new Jedis("localhost")
    val conf = new SparkConf().setMaster("local").setAppName("demandByHour")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile(args(0))
      .map(_.split("	")).map(p => DEMAND_DATA(p(0),p(4),p(7).toInt,p(3).toLong,p(6))).toDF()
    data.registerTempTable("demand_origin")

    val showDict = sc.textFile(args(1))
      .map(_.split("	")).map ( p =>  SHOW_TYPE(p(0),p(1))).toDF()
    showDict.registerTempTable("show_dict")

    //处理时统计、总的时长和请求次数
    sqlContext.sql(
      """
        select uid,day,hour,sum(remain_time)/60 remain_time,count(*) request_times
        from demand_origin group by uid,day,hour
      """.stripMargin
    ).registerTempTable("demand_hour_temp")
    sqlContext.sql(
      """
        select day,hour,sum(remain_time) remain_time,sum(request_times) request_times,count(*) usernum
        from demand_hour_temp group by day,hour
      """.stripMargin
    ).registerTempTable("demand_hour_sum")

    //按频道统计并合并节目类型
    sqlContext.sql(
      """
        select day,hour,channel_name,count(distinct uid) as usernum,sum(remain_time)/60 remain_time,count(*) request_times
        from demand_origin group by day,hour,channel_name
      """.stripMargin
    ).registerTempTable("demand_channel_temp")
    sqlContext.sql(
      """
        select a.*,b.show_type from demand_channel_temp a,show_dict b where a.channel_name = b.show_name
      """.stripMargin
    ).registerTempTable("demand_channel")

    //本次需要计算的日 4-9月共183天
    val start = DateUtils.parseDate("2016-04-01", "yyyy-MM-dd")
    (0 to 182).foreach(i => {
      val day = DateFormatUtils.format(DateUtils.addDays(start, i), "yyyy-MM-dd")
      for (hour <- 0 to 23 ) {
        println(">>> Process Demand Day:" + day + " Hour:" + hour)
        val sumDF = sqlContext.sql("select * from demand_hour_sum where day='"+day+"' and hour="+hour)
        if(sumDF.count() > 0) {
          val sum = sumDF.first()
          val remainTime = sum.getDouble(1)
          val requestTimes = sum.getLong(2)
          val userNum = sum.getLong(3)

          //      //用户概况和行为
          val coverUserNum = sqlContext.sql("select count(distinct(uid)) from demand_origin where day<='" + day + "' and hour<="+hour)
            .first().getLong(0)

          val coverPct = userNum * 1.0 / coverUserNum
          val timeUseAVG = remainTime / userNum
          val requestAVG = requestTimes * 1.0 / userNum
          val requestOne = remainTime / requestTimes

          val summary = day + "\t" + hour + "\t" + 2 + "\t" + userNum + "\t" + coverPct + "\t" + remainTime + "\t" + timeUseAVG + "\t" + requestTimes + "\t" + requestAVG + "\t" + requestOne
          jedis.sadd("SUMMARY_T", summary)
          println(">>> Complete SummaryT:"+summary)

          //点播频道类型
          val allShow = sqlContext.sql("select count(distinct(t.channel_name)) channel_name_num " +
            "from demand_channel t where t.day='" + day + "' and t.hour="+hour).first()
          val allShowNum = allShow.getLong(0) //所有的节目数量
          val FIX_TIME = 60

          //电影类
          val movieDF = sqlContext.sql("select count(distinct(t.channel_name)) channel_name_num, sum(t.remain_time) remain_time, sum(t.usernum) usernum " +
            "from demand_channel t where t.day='" + day + "' and t.hour="+hour+" and t.show_type='电影'")
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

            val demand_movie = day+"\t"+hour+"\t"+"电影"+"\t"+movieUserIndex+"\t"+movieCoverPct+"\t"+movieMarketPct+"\t" +
              movieTimeUseAVG+"\t"+movieRemainTime+"\t"+movieUserNum+"\t"+movieShowRatio+"\t"+remainTime+"\t"+userNum
            jedis.sadd("DEMAND_T", demand_movie)
            println(">>> Complete demandW_movie:"+demand_movie)
          }

          //电视剧类
          val tvDF = sqlContext.sql("select count(distinct(t.channel_name)) channel_name_num, sum(t.remain_time) remain_time, sum(t.usernum) usernum " +
            "from demand_channel t where t.day='" + day + "' t.hour="+hour+" and t.show_type='电视剧'")
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

            val demand_tv = day+"\t"+hour+"\t"+"电视剧"+"\t"+tvUserIndex+"\t"+tvCoverPct+"\t"+tvMarketPct+"\t" +
              tvTimeUseAVG+"\t"+tvRemainTime+"\t"+tvUserNum+"\t"+tvShowRatio+"\t"+remainTime+"\t"+userNum
            jedis.sadd("DEMAND_T", demand_tv)
            println(">>> Complete demandT_tv:"+demand_tv)
          }

          //点播节目
          val shows = sqlContext.sql("select t.channel_name,sum(t.remain_time) remain_time, sum(t.usernum) usernum" +
            " from demand_channel t where t.show_type = '电视剧' or t.show_type = '电影' and t.day='" + day + "' and t.hour="+hour+" group by t.channel_name").collect()
          if(shows.size > 0) {
            shows.foreach(show => {
              val showName = show.getString(0)
              val showRemainTime = show.getDouble(1)
              val showUserNum = show.getLong(2)
              val showType = sqlContext.sql("select show_type from show_dict where show_name='" + showName + "'").first().getString(0)
              val showMarketPct = showRemainTime / remainTime
              val showCoverPct = showUserNum * 1.0 / userNum
              val showUserIndex = showRemainTime / FIX_TIME / coverUserNum * USER_INDEX_OFFSET
              val showTimeUseAVG = showRemainTime / showUserNum

              val demandShows = day+"\t"+hour+"\t"+showName+"\t"+showType+"\t"+showUserIndex+"\t"+showCoverPct+"\t"+showMarketPct+"\t" +
                showTimeUseAVG+"\t"+showRemainTime+"\t"+showUserNum+"\t"+remainTime+"\t"+userNum
              jedis.sadd("DEMAND_SHOWS_T", demandShows)
              println(">>> Complete DEMAND_SHOWS_T:"+demandShows)
            })
          }
        }
      }
    })
  }
}
