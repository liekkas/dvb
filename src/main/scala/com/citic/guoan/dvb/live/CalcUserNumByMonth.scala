package com.citic.guoan.dvb.live

import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Created by liekkas on 16/10/17.
  */
object CalcUserNumByMonth {
  case class LIVE_DATA(uid:String,month:Int,week:Int,day:String,time_in_use:Long)

  def main(args: Array[String]): Unit = {
    val jedis = new Jedis(args(3))
    val conf = new SparkConf().setAppName("CalcUserNumByMonth")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LIVE_DATA]))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val liveData = sc.textFile(args(0))
      .map(_.split("	"))
      .map(p => LIVE_DATA(p(0),p(1).toInt,p(2).toInt,p(3),p(7).toLong))
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

      liveData.filter(p => p.month == month)
        .map(_.uid)
        .distinct()
        .foreachPartition(part => {
          val j = new Jedis(args(3))
          part.foreach(uid => {
            j.sadd(month+MONTH, uid)
            j.sadd(TOTAL, uid)
          })
        })

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
        coverUserNum+"\t"+userInNum+"\t"+userOutNum+"\t"+lastUserNum

      jedis.sadd(RESULT,result)
      jedis.del(lastMonth+MONTH)
    })
    jedis.del(201609+MONTH)
    jedis.del(TOTAL)
    println(">>> Completed Months")
  }
}
