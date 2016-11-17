package com.citic.guoan.dvb.live

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/11/17.
  */
object ExtractChannels {
  case class LIVE_DATA(month:Int,channel:String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ExtractChannels")
    val sc = new SparkContext(conf)

    sc.textFile(args(0)).map(p => {
        val arr = p.split("\t")
        LIVE_DATA(arr(1).toInt,arr(5))
      })
      .filter(p => p.month > 201603 && p.month < 201610)
      .map(_.channel)
      .distinct().coalesce(1).saveAsTextFile(args(1))
  }
}
