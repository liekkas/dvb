package com.citic.guoan.dvb.live

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/10/26.
  */
object CleanLiveData {
//  case class LIVE(code_type:String,uid:String,date:Long,channel_name:String,program_name:String,
//                  month:Int,week:Int,day:String,hour:Int)
  case class LIVE(uid:String,date:String,channel_name:String,program_name:String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CleanLiveData")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val origin = sc.textFile(args(0)).map(_.split(";")) //分解出地区|采集系统ID|内容

    //过滤直播和心跳的数据,同时没有日期的和没有id标识的都过滤掉
    def filterFunc(line: Array[String]): Boolean = {
//      (line(0) == "0201|0999|01" || line(0) == "0201|0999|11") &&
      line(0) == "0201|0999|01" && line(1) != "" && line(2) != ""
    }

    //直播数据
    origin
      .filter(filterFunc)
      .map(p => LIVE(p(2),p(1),if(p(7)=="") "-1" else p(7),if(p(8)=="") "-1" else p(8)))
      .toDF().sort("uid","date")
      .map(f => f(0) + "\t" +f(1) + "\t" +f(2) + "\t" +f(3))
      .repartition(1).saveAsTextFile(args(1)+"/live")
  }
}
