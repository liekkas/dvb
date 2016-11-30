package com.citic.guoan.dvb.live.prepare

import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Created by liekkas on 16/10/17.
  */
object CalcPrepareSumByDay {
  case class LIVE_DATA(uid:String,day:String,time_in_use:Long)

  def main(args: Array[String]): Unit = {
    val jedis = new Jedis(args(2))
    val conf = new SparkConf().setAppName("CalcPrepareSumByDay")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LIVE_DATA]))
    val sc = new SparkContext(conf)

    val liveData = sc.textFile(args(0))
      .filter(_ != "")
      .map(_.split("\t"))
      .map(p => LIVE_DATA(p(0),p(3),p(7).toInt))
    liveData.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val TOTAL = "TOTAL"
    val DAY = "DAY"
    val RESULT = "RESULT"

    //导入以前的uid
    val prepareData = sc.textFile(args(1)).collect()
    prepareData.foreach(uid => {
      jedis.sadd(TOTAL, uid)
    })
    prepareData.take(120000).foreach(uid => jedis.sadd("2016-03-31"+DAY,uid))

    //计算月覆盖用户数、流入、流出用户数
    val start = DateUtils.parseDate("2016-04-01", "yyyy-MM-dd")
    (0 to 182).foreach(i => {
      val day = DateFormatUtils.format(DateUtils.addDays(start, i), "yyyy-MM-dd")
      println(">>> Process Live Day:" + day)
      val lastDay = DateFormatUtils.format(DateUtils.addDays(start, i-1),"yyyy-MM-dd")

      val data = liveData.filter(p => p.day == day)
      data.persist(StorageLevel.MEMORY_AND_DISK_SER)

      data.map(_.uid)
        .distinct()
        .foreachPartition(part => {
          val j = new Jedis(args(2))
          part.foreach(uid => {
            j.sadd(day+DAY, uid)
            j.sadd(TOTAL, uid)
          })
        })

      //使用时长--分钟
      val timeInUse = data.map(_.time_in_use).sum() / 60

      //用户数
      val userNum = jedis.smembers(day+DAY).size()
      //覆盖用户数
      val coverUserNum = jedis.smembers(TOTAL).size()
      //流入用户数 - 昨天没在线今天在线
      val userInNum = jedis.sdiff(day+DAY,lastDay+DAY).size()
      //流出用户数 - 昨天在线今天没在线
      val userOutNum = jedis.sdiff(lastDay+DAY,day+DAY).size()
      //上个时间的用户数
      val lastUserNum = jedis.smembers(lastDay+DAY).size()

      //最终结果保存到文本中,供后续计算使用
      val result = "day" + "\t" +day + "\t" + userNum + "\t" +
        coverUserNum+"\t"+userInNum+"\t"+userOutNum+"\t"+lastUserNum+"\t"+"%.6f".format(timeInUse)

      jedis.sadd(RESULT,result)
      jedis.del(lastDay+DAY)
    })
    jedis.del("2016-09-30"+DAY)
    jedis.del(TOTAL)
    println(">>> Completed Days")
  }
}
