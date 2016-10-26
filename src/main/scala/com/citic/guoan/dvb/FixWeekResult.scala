package com.citic.guoan.dvb

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/10/25.
  * 原始数据第一周从1月1号开始算的,但是根据国际规则
  * https://zh.wikipedia.org/wiki/ISO%E9%80%B1%E6%97%A5%E6%9B%86
  * 第一周至少有4天,所以其实2016年1月4号算第一周,实际上手机日历和大部分js日历组件都是按这个标准算的
  * 所以需要对按周计算出来的结果对周进行加1
  *
  */
object FixWeekResult {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("fixWeekResult")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.textFile(args(0))
      .map(_.split("	"))
      .map(f => (f(0).toInt-1) + "\t" + f(1) + "\t" +
        f(2) + "\t" + f(3) + "\t" + f(4) +"\t" + f(5) + "\t"+
        f(6) + "\t" + f(7) + "\t" + f(8) + "\t" + f(9)+ "\t" + f(10) + "\t" + f(11))
      .repartition(1).saveAsTextFile(args(3)+"/demandByWeek/summary")

    sc.textFile(args(1))
      .map(_.split("	"))
      .map(f => (f(0).toInt-1) + "\t" + f(1) + "\t" +
        f(2) + "\t" + f(3) + "\t" + f(4) + "\t" +
        f(5) + "\t" + f(6) + "\t" + f(7) + "\t" +
        f(8) + "\t" + f(9) + "\t" + f(10)
      ).repartition(1).saveAsTextFile(args(3)+"/demandByWeek/showType")

    sc.textFile(args(2))
      .map(_.split("	"))
      .map(f => (f(0).toInt-1) + "\t" + f(1) + "\t" + f(2)+ "\t" +
        f(3) + "\t" + f(4) + "\t" + f(5) + "\t" +
        f(6) + "\t" + f(7) + "\t" + f(8) + "\t" + f(9) + "\t" + f(10)
      ).repartition(1).saveAsTextFile(args(3)+"/demandByWeek/show")

    println(">>> Complete!")
  }
}
