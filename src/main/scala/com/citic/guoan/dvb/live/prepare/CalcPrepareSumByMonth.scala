package com.citic.guoan.dvb.live.prepare

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Created by liekkas on 16/10/17.
  */
object CalcPrepareSumByMonth {
  case class LIVE_DATA(uid:String,month:Int,time_in_use:Long)

  def main(args: Array[String]): Unit = {
    val jedis = new Jedis(args(2))
    val conf = new SparkConf().setAppName("CalcPrepareSumByMonth")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LIVE_DATA]))
    val sc = new SparkContext(conf)

    val liveData = sc.textFile(args(0))
      .filter(_ != "")
      .map(_.split("\t"))
      .map(p => LIVE_DATA(p(0),p(1).toInt,p(7).toLong))
    liveData.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val TOTAL = "TOTAL"
    val MONTH = "MONTH"
    val RESULT = "RESULT"

    //导入以前的uid
    val prepareData = sc.textFile(args(1)).collect()
    prepareData.foreach(uid => {
      jedis.sadd(TOTAL, uid)
      jedis.sadd(201603+MONTH,uid)
    })

    //计算月覆盖用户数、流入、流出用户数
    val months = 201604 to 201609
    months.foreach(month => {
      println(">>> Process Live Month:" + month)
      val lastMonth = month - 1

      val data = liveData.filter(p => p.month == month)
      data.persist(StorageLevel.MEMORY_AND_DISK_SER)

      data.map(_.uid)
        .foreachPartition(part => {
          val j = new Jedis(args(2))
          part.foreach(uid => {
            j.sadd(month+MONTH, uid)
            j.sadd(TOTAL, uid)
          })
        })

      //使用时长--分钟
      val timeInUse = data.map(_.time_in_use).sum() / 60

      //用户数
      val userNum = jedis.smembers(month+MONTH).size()
      //覆盖用户数
      val coverUserNum = jedis.smembers(TOTAL).size()
      //流入用户数 - 昨天没在线今天在线
      val userInNum = jedis.sdiff(month+MONTH,lastMonth+MONTH).size()
      //流出用户数 - 昨天在线今天没在线
      val userOutNum = jedis.sdiff(lastMonth+MONTH,month+MONTH).size()
      //上个时间的用户数
      val lastUserNum = jedis.smembers(lastMonth+MONTH).size()

      //最终结果保存到文本中,供后续计算使用
      val result = "month" + "\t" +month + "\t" + userNum + "\t" +
        coverUserNum+"\t"+userInNum+"\t"+userOutNum+"\t"+lastUserNum+"\t"+"%.6f".format(timeInUse)

      jedis.sadd(RESULT,result)
      jedis.del(lastMonth+MONTH)
    })
    jedis.del(201609+MONTH)
    jedis.del(TOTAL)
    println(">>> Completed Months")
  }
}
