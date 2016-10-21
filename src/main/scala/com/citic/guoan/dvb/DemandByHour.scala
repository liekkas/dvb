package com.citic.guoan.dvb

import java.io.File

import org.apache.commons.io.FileUtils
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
  case class UID_COUNT(date_type:String,date:String,user_num:Long,cover_user_num:Long,user_in_num:Long,user_out_num:Long)

  def main(args: Array[String]): Unit = {
    val summaryTFile = new File(args(3) + File.separator + "T_USER_SUMMARY_T")
    val demandTFile = new File(args(3) + File.separator + "T_DEMAND_BROADCAST_T")
    val demandShowsTFile = new File(args(3) + File.separator + "T_DEMAND_BROADCAST_SHOWS_T")

    val USER_INDEX_OFFSET = 1000
//    val conf = new SparkConf().setMaster("local").setAppName("demandByHour")
    val conf = new SparkConf().setAppName("demandByHour")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile(args(0))
      .map(_.split("	"))//.filter(p => p(4) > "2016-03-31") //过滤掉用不着的数据
      .map(p => DEMAND_DATA(p(0),p(2),p(4).toInt,p(1).toLong,p(3))).toDF().cache()
    val showDict = sc.textFile(args(1))
      .map(_.split("	")).map ( p =>  SHOW_TYPE(p(0),p(1))).toDF().cache()
    //加入节目类型 -- 这块比较耗时,如果原始数据能提供更好
    data.join(showDict, data("channel_name")===showDict("show_name"), "left")
      .select("uid","day","hour","remain_time","channel_name","show_type")
      .registerTempTable("demand_origin")

    sc.textFile(args(2))
      .map(_.split("	")).filter(p => p(0).equals("day"))
      .map ( p =>  UID_COUNT(p(0),p(1),p(2).toLong,p(3).toLong,p(4).toLong,p(5).toLong)).toDF()
      .registerTempTable("uid_count")

    //按时统计
    sqlContext.sql(
      """
        select day,sum(remain_time)/60 remain_time,count(*) request_times,count(distinct uid) as usernum, hour
        from demand_origin group by day,hour
      """.stripMargin
    ).registerTempTable("demand_hour_sum")

    //按节目类型
    sqlContext.sql(
      """
        select day,show_type,count(distinct uid) as usernum,sum(remain_time)/60 remain_time,count(distinct channel_name) as shownum, hour
        from demand_origin group by day,hour,show_type
      """.stripMargin
    ).registerTempTable("demand_show_type")

    //按节目
    sqlContext.sql(
      """
        select day,show_type,channel_name,count(distinct uid) as usernum,sum(remain_time)/60 remain_time, hour
        from demand_origin group by day,hour,show_type,channel_name
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

          //      //用户概况和行为 小时的是当天的覆盖用户数,所以都相等
          val uidCount = sqlContext.sql("select cover_user_num from uid_count where date='" + day + "'").first()
          val coverUserNum = uidCount.getLong(0)

          val coverPct = userNum * 1.0 / coverUserNum
          val timeUseAVG = remainTime / userNum
          val requestAVG = requestTimes * 1.0 / userNum
          val requestOne = remainTime / requestTimes

          val summary = day + "\t" + hour + "\t" + 2 + "\t" + userNum + "\t" + "%.4f".format(coverPct) + "\t" +
            "%.4f".format(remainTime) + "\t" + "%.4f".format(timeUseAVG) + "\t" + requestTimes + "\t" + "%.4f".format(requestAVG) + "\t" + "%.4f".format(requestOne)+"\n"
          FileUtils.writeStringToFile(summaryTFile,summary,true)
          println(">>> Complete SummaryT:"+summary)

          //点播频道类型
          val allShow = sqlContext.sql("select sum(shownum) " +
            "from demand_show_type t where t.day='" + day + "' and t.hour="+hour).first()
          val allShowNum = allShow.getLong(0) //所有的节目数量
          val FIX_TIME = 60

          Array("电影","电视剧").foreach(item => {
            //按节目类型
            val tvDF = sqlContext.sql("select shownum, remain_time, usernum " +
              "from demand_show_type t where t.day='" + day + "' and t.hour="+hour+" and t.show_type='"+item+"'")
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

              val demandTv = day + "\t" + hour + "\t" + item + "\t" + "%.4f".format(tvUserIndex) + "\t" + "%.4f".format(tvCoverPct) + "\t" + "%.4f".format(tvMarketPct) + "\t" +
                "%.4f".format(tvTimeUseAVG) + "\t" + "%.4f".format(tvRemainTime) + "\t" + tvUserNum + "\t" + "%.4f".format(tvShowRatio) + "\t" + "%.4f".format(remainTime) + "\t" + userNum+"\n"

              FileUtils.writeStringToFile(demandTFile,demandTv,true)
              println(">>> Complete demandT_tv:"+demandTv)
            }else{
              val demandTv = day+"\t"+hour + "\t" + "item" + "\t" + 0.00 + "\t" + 0.00 + "\t" + 0.00 + "\t" +
                0.00 + "\t" + 0.00 + "\t" + 0 + "\t" + 0.00 + "\t" + "%.4f".format(remainTime) + "\t" + userNum+"\n"
              FileUtils.writeStringToFile(demandTFile,demandTv,true)
            }

            //按具体节目
            val shows = sqlContext.sql("select channel_name,remain_time, show_type, usernum" +
              " from demand_channel where show_type = '" + item + "' and t.day='" + day + "' and t.hour="+hour).collect()
            if (shows.size > 0) {
              shows.foreach(show => {
                val showName = show.getString(0)
                val showRemainTime = show.getDouble(1)
                val showType = show.getString(2)
                val showUserNum = show.getLong(3)
                val showMarketPct = showRemainTime / remainTime
                val showCoverPct = showUserNum * 1.0 / userNum
                val showUserIndex = showRemainTime / FIX_TIME / coverUserNum * USER_INDEX_OFFSET
                val showTimeUseAVG = showRemainTime / showUserNum

                val demandShow = day+"\t"+hour+"\t"+showName+"\t"+showType+"\t"+"%.4f".format(showUserIndex)+"\t"+"%.4f".format(showCoverPct)+"\t"+"%.4f".format(showMarketPct)+"\t" +
                  "%.4f".format(showTimeUseAVG)+"\t"+"%.4f".format(showRemainTime)+"\t"+showUserNum+"\t"+"%.4f".format(remainTime)+"\t"+userNum+"\n"

                FileUtils.writeStringToFile(demandShowsTFile,demandShow,true)
              })
            }
          })
        }
      }
    })
  }
}
