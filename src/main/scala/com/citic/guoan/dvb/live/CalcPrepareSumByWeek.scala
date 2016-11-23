package com.citic.guoan.dvb.live

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Created by liekkas on 16/10/17.
  */
object CalcPrepareSumByWeek {
  case class LIVE_DATA(uid:String,week:Int,time_in_use:Long)

  def main(args: Array[String]): Unit = {
    val jedis = new Jedis(args(3))
    val conf = new SparkConf().setAppName("CalcUserNumByWeek")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LIVE_DATA]))
    val sc = new SparkContext(conf)

    val liveData = sc.textFile(args(0))
      .map(_.split("	"))
      .map(p => LIVE_DATA(p(0),p(2).toInt,p(7).toLong))
    liveData.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val TOTAL = "TOTAL"
    val WEEK = "WEEK"
    val RESULT = "RESULT"

    //导入以前的uid
    val prepareData = sc.textFile(args(1)).collect()
    prepareData.foreach(uid => {
      jedis.sadd(TOTAL, uid)
    })
    prepareData.take(170000).foreach(uid => jedis.sadd(201612+WEEK,uid))

    //计算月覆盖用户数、流入、流出用户数
    val weeks = 201613 to 201639
    weeks.foreach(week => {
      println(">>> Process Live Week:" + week)
      val lastWeek = week - 1

      val data = liveData.filter(p => p.week == week)
      data.persist(StorageLevel.MEMORY_AND_DISK_SER)

      data.map(_.uid)
        .distinct()
        .foreachPartition(part => {
          val j = new Jedis(args(3))
          part.foreach(uid => {
            j.sadd(week+WEEK, uid)
            j.sadd(TOTAL, uid)
          })
        })

      //使用时长--分钟
      val timeInUse = data.map(_.time_in_use).sum() / 60

      //用户数
      val userNum = jedis.smembers(week+WEEK).size()
      //覆盖用户数
      val coverUserNum = jedis.smembers(TOTAL).size()
      //流入用户数 - 昨天没在线今天在线
      val userInNum = jedis.sdiff(week+WEEK,lastWeek+WEEK).size()
      //流出用户数 - 昨天在线今天没在线
      val userOutNum = jedis.sdiff(lastWeek+WEEK,week+WEEK).size()
      //上个时间的用户数
      val lastUserNum = jedis.smembers(lastWeek+WEEK).size()

      //最终结果保存到文本中,供后续计算使用
      val result = "week" + "\t" +week + "\t" + userNum + "\t" +
        coverUserNum+"\t"+userInNum+"\t"+userOutNum+"\t"+lastUserNum+"\t"+"%.6f".format(timeInUse)

      jedis.sadd(RESULT,result)
      jedis.del(lastWeek+WEEK)
    })
    jedis.del(201639+WEEK)
    jedis.del(TOTAL)
    println(">>> Completed Weeks")
  }
}
