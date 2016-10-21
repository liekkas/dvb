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
object DemandPrepare {
  case class DEMAND_DATA(uid:String,month:Int,week:Int,day:String,remain_time:Long)

  def main(args: Array[String]): Unit = {
    val resultFile = new File(args(1) + File.separator + "demand_uid_count.txt")

    //计算中间结果先放到redis中,最后一并导出文本
    val jedis = new Jedis("localhost")
    val conf = new SparkConf().setMaster("local").setAppName("demandPrepare")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile(args(0))
      .map(_.split("	")).map(p => DEMAND_DATA(p(0),p(6).toInt,p(5).toInt,p(2),p(1).toLong)).toDF()
      .registerTempTable("demand_origin")

    val MONTH = "DEMAND_MONTH"
    val WEEK = "DEMAND_WEEK"
    val DAY = "DEMAND_DAY"

    //计算月覆盖用户数、流入、流出用户数
    val months = 201604 to 201609
    months.foreach(month => {
      println(">>> Process Demand Month:" + month)
      val lastMonth = month - 1

      //假如上个月未计算,则先求上个时刻的,通常只在第一个月算一遍
      if(!jedis.exists(lastMonth+MONTH)) {
        //先求上个时刻的用户数,并保存到redis中
        sqlContext.sql("select distinct(uid) from demand_origin where month=" + lastMonth)
            .collect().foreach(uid => {
            jedis.sadd(lastMonth + MONTH, uid.getString(0).toString)
            jedis.sadd(MONTH, uid.getString(0))
          })
      }

      //本月用户数
      val users = sqlContext.sql("select distinct(uid) from demand_origin where month=" + month)
          .collect()
      users.foreach(uid => {
          jedis.sadd(month+MONTH, uid.getString(0))
          jedis.sadd(MONTH, uid.getString(0))
        })

      //覆盖用户数
      val coverUserNum = jedis.smembers(MONTH).size()
      //流入用户数 - 昨天没在线今天在线
      val userInNum = jedis.sdiff(month+MONTH,lastMonth+MONTH).size()
      //流出用户数 - 昨天在线今天没在线
      val userOutNum = jedis.sdiff(lastMonth+MONTH,month+MONTH).size()
      //最终结果保存到文本中,供后续计算使用
      val result = "month" + "\t" +month + "\t" + users.size + "\t" + coverUserNum+"\t"+userInNum+"\t"+userOutNum+"\n"
      FileUtils.writeStringToFile(resultFile,result,true)

      jedis.del(lastMonth+MONTH)
    })
    jedis.del(MONTH)

    //计算周覆盖用户数、流入、流出用户数
    val weeks = 201614 to 201640
    weeks.foreach(week => {
      println(">>> Process Demand Week:" + week)
      val lastWeek = week - 1

      //假如上个周未计算,则先求上个时刻的,通常只在第一个周算一遍
      if(!jedis.exists(lastWeek+WEEK)) {
        //先求上个时刻的用户数,并保存到redis中
        sqlContext.sql("select distinct(uid) from demand_origin where week=" + lastWeek)
          .collect().foreach(uid => {
          jedis.sadd(lastWeek + WEEK, uid.getString(0).toString)
          jedis.sadd(WEEK, uid.getString(0))
        })
      }

      //本周用户数
      val users = sqlContext.sql("select distinct(uid) from demand_origin where week=" + week)
        .collect()
      users.foreach(uid => {
        jedis.sadd(week+WEEK, uid.getString(0))
        jedis.sadd(WEEK, uid.getString(0))
      })
      //覆盖用户数
      val coverUserNum = jedis.smembers(WEEK).size()
      //流入用户数 - 昨天没在线今天在线
      val userInNum = jedis.sdiff(week+WEEK,lastWeek+WEEK).size()
      //流出用户数 - 昨天在线今天没在线
      val userOutNum = jedis.sdiff(lastWeek+WEEK,week+WEEK).size()
      //最终结果保存到文本中,供后续计算使用
      val result = "week" + "\t" +week + "\t" + users.size + "\t" + coverUserNum+"\t"+userInNum+"\t"+userOutNum+"\n"
      FileUtils.writeStringToFile(resultFile,result,true)

      jedis.del(lastWeek+WEEK)
    })
    jedis.del(WEEK)

    //计算天覆盖用户数、流入、流出用户数
    val start = DateUtils.parseDate("2016-04-01", "yyyy-MM-dd")
    (0 to 182).foreach(i => {
      val day = DateFormatUtils.format(DateUtils.addDays(start, i), "yyyy-MM-dd")
      println(">>> Process Demand Day:" + day)
      val lastDay = DateFormatUtils.format(DateUtils.addDays(start, i-1),"yyyy-MM-dd")

      //假如上个天未计算,则先求上个时刻的,通常只在第一个天算一遍
      if(!jedis.exists(lastDay+DAY)) {
        //先求上个时刻的用户数,并保存到redis中
        sqlContext.sql("select distinct(uid) from demand_origin where day='" + lastDay + "'")
          .collect().foreach(uid => {
          jedis.sadd(lastDay + DAY, uid.getString(0).toString)
          jedis.sadd(DAY, uid.getString(0))
        })
      }

      //本天用户数
      val users = sqlContext.sql("select distinct(uid) from demand_origin where day='" + day + "'")
        .collect()
      users.foreach(uid => {
        jedis.sadd(day+DAY, uid.getString(0))
        jedis.sadd(DAY, uid.getString(0))
      })
      //覆盖用户数
      val coverUserNum = jedis.smembers(DAY).size()
      //流入用户数 - 昨天没在线今天在线
      val userInNum = jedis.sdiff(day+DAY,lastDay+DAY).size()
      //流出用户数 - 昨天在线今天没在线
      val userOutNum = jedis.sdiff(lastDay+DAY,day+DAY).size()
      //最终结果保存到文本中,供后续计算使用
      val result = "day" + "\t" +day + "\t" + users.size + "\t" + coverUserNum+"\t"+userInNum+"\t"+userOutNum+"\n"
      FileUtils.writeStringToFile(resultFile,result,true)

      jedis.del(lastDay+DAY)
    })
    jedis.del(DAY)
  }
}
