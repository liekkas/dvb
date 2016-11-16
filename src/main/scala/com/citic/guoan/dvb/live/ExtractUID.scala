package com.citic.guoan.dvb.live

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/11/14.
  * extract uids from April/May
  * 4月取arg(2)个数,5月取arg(3)个数,而且不能重复
  */
object ExtractUID {
  case class LIVE_DATA(uid:String,month:Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ExtractUID")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //计算calced的
//    sc.textFile(args(0))
//      .map(p => {
//        val arr = p.split("\t")
//        LIVE_DATA(arr(0),arr(1).toInt)
//      })
//      .toDF().registerTempTable("live")

    //计算filtered的
//    sc.textFile(args(0))
//      .map(p => {
//        val arr = p.split("\t")
//        LIVE_DATA(arr(0),arr(1).substring(0,6).toInt)
//      })
//      .toDF().registerTempTable("live")
//
//    val result = sqlContext.sql(
//      """
//        |select count(distinct(uid)) num, month
//        |  from live
//        | group by month
//      """.stripMargin)
//
//    result.show()
//
//    result.map(f => f(1) + "\t" + f(0)).repartition(1).saveAsTextFile(args(2))

    val initData = sc.textFile(args(0))
      .map(p => {
        val arr = p.split("\t")
        Array(arr(0),arr(1))
      })

    val result = initData
      .filter(p => p(1).toInt == 201604 || p(1).toInt == 201605)
      .map(_(0)).distinct().take(args(1).toInt)

    sc.makeRDD(result).map(_.mkString).repartition(1).saveAsTextFile(args(2))
  }
}
