package com.citic.guoan.dvb.live

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/11/14.
  * 计算过滤后的用户数,按月份排列
  */
object CalcFilteredUserNum {
  case class LIVE_DATA(uid:String,month:Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CalcFilteredUserNum")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //过滤直播和心跳的数据,同时没有日期的和没有id标识的都过滤掉
    //过滤掉日期格式不正常的(不足14位数),和用户id不正常的(不足6位数)
    //过滤掉日期不再20164月到9月之间的数据
    def filterFunc(line: Array[String]): Boolean = {
      line.length >= 9 && line(0) == "0201|0999|01" && line(1) != "" && line(2) != "" &&
        line(1).length == 14 && line(2).length > 6 &&
          line(1).substring(0,6).toInt > 201603 && line(1).substring(0,6).toInt < 201610
    }

    sc.textFile(args(0))
      .map(_.split(";")).filter(filterFunc)
      .map(p => LIVE_DATA(p(2),p(1).substring(0,6).toInt))
      .toDF().registerTempTable("live")

    val result = sqlContext.sql(
      """
        |select count(distinct(uid)) num, month
        |  from live
        | group by month
      """.stripMargin)

      result.map(f => f(1) + "\t" + f(0)).repartition(1).saveAsTextFile(args(2))
  }
}
