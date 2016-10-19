package com.citic.guoan.dvb

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object AllTime {
  case class DVB(uid:String,date:String,time_frame:Int,remain_time:Double,channel_name:String)
  case class DVB_TYPE(channel_name:String,channel_type:String)
  def main(args: Array[String]){
       val conf = new SparkConf().setMaster("local").setAppName("dvb")
       val sc = new SparkContext(conf)
       val sqlContext = new SQLContext(sc)
       import sqlContext.implicits._
       val data = sc.textFile("/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/dvb.txt")
         .map(_.split(" ")).map ( p =>  DVB(p(0),p(4),p(7).toInt,p(3).toDouble,p(6))).toDF()
       data.registerTempTable("dvb")

       val channel_type = sc.textFile("/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/channel_dict.txt")
         .map(_.split("	")).map ( p =>  DVB_TYPE(p(0),p(1))).toDF()

       //DataFrame
       val all_result = sqlContext.sql("select date,time_frame,sum(remain_time) as all_time_use,count(distinct uid) as all_user_num from dvb group by date,time_frame")
       all_result.registerTempTable("all_result_tab")
       val result = sqlContext.sql("select date,time_frame,channel_name,avg(remain_time) as time_use_avg,sum(remain_time) as time_use_sum,count(distinct uid) as user_num from dvb group by date,time_frame,channel_name")
       result.registerTempTable("result_tab")
       val output=sqlContext.sql("select r.date,r.time_frame,r.channel_name as c_name,r.time_use_avg,r.time_use_sum,r.user_num,a.all_time_use,a.all_user_num from result_tab r left join all_result_tab a on r.date=a.date and r.time_frame=a.time_frame")
       val out = output.join(channel_type, output("c_name")===channel_type("channel_name"), "left")
                                   .select("date","time_frame","channel_name","channel_type","time_use_avg","time_use_sum","user_num","all_time_use","all_user_num")
//       out.write.format("parquet").save("/Users/howard/work/result.parquet")
//       out.write.mode(org.apache.spark.sql.SaveMode.Overwrite).format("json").save("/Users/howard/work/result.json")
//        out.write.mode(org.apache.spark.sql.SaveMode.Append).saveAsTable("tab") //HiveContext
        out.show()
        out.rdd.repartition(1).saveAsTextFile("/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/result")
       println("---------------->")
  }
  
}