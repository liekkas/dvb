package com.citic.guoan.dvb.live.calckpi

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by liekkas on 16/11/17.
  */
object SortShowsByUserIndex {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortShowsByUserIndex")
    val sc = new SparkContext(conf)
    sc.textFile(args(0))
      .sortBy(p => p.split("\t")(5).toDouble, false)
      .coalesce(1)
      .saveAsTextFile(args(1))
  }
}
