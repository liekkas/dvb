package com.citic.guoan.dvb.live

import com.citic.guoan.dvb.partitioner.ViewerIdPartitioner
import org.apache.hadoop.io.compress.{GzipCodec}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/10/26.
  */
object FilterData {
  case class LIVE(uid:String,date:String,channel_name:String,program_name:String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FilterData")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LIVE]))
    val sc = new SparkContext(conf)

    //过滤直播和心跳的数据,同时没有日期的和没有id标识的都过滤掉
    def filterFunc(line: Array[String]): Boolean = {
      line(0) == "0201|0999|01" && line(1) != "" && line(2) != "" && line.length >= 9
    }

    sc.textFile(args(0))
      .map(_.split(";")) //分解出地区|采集系统ID|内容
      .filter(filterFunc) //过滤留下直播数据
      .map(p => { //生成pair,使用uid+date当key,切分时按uid进行,date用于后面的repartitionAndSortWithinPartitions算子
        val channel = if(p(7)=="" || p(7)=="null") "?" else p(7)
        //去掉频道后面带括号的部分比如(可回看)(付费)等等
        val channelFixed = if(channel.indexOf("(") > -1) channel.substring(0, channel.indexOf("(")) else channel
        val program = if(p(8)=="" || p(8)=="null") "?" else p(8)
        (p(2)+","+p(1),LIVE(p(2),p(1),channelFixed,program))
      })
      .repartitionAndSortWithinPartitions(new ViewerIdPartitioner(args(2).toInt)) //重新分区并按UID和时间排序
      .map(f => f._2.uid + "\t" +f._2.date + "\t" +f._2.channel_name + "\t" +f._2.program_name)
      .saveAsTextFile(args(1),classOf[GzipCodec])
//      .toDF().write.parquet(args(1)+"parquet")
  }
}
