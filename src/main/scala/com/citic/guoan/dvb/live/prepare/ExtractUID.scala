package com.citic.guoan.dvb.live.prepare

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/11/14.
  * extract uids 从指定月份
  */
object ExtractUID {
  case class LIVE_DATA(uid:String,month:Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ExtractUID")
    val sc = new SparkContext(conf)

    val initData = sc.textFile(args(0))
      .map(p => {
        val arr = p.split("\t")
        Array(arr(0),arr(1))
      })

    val result = initData
      .filter(p => p(1) == args(3) || p(1) == args(4))
      .map(_(0)).distinct().take(args(2).toInt)

    sc.makeRDD(result).map(_.mkString).repartition(1).saveAsTextFile(args(1))
  }
}
