package com.citic.guoan.dvb

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liekkas on 16/11/16.
  */
object TransformData {
  case class DATA(key:String,value:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransformData")
    val sc = new SparkContext(conf)

    sc.textFile(args(0)).map(p => {
      val arr = p.split(";")
      arr(0) match {
        case "0301|0204|03" |
             "0301|0305|01" | "0301|0305|02" | "0301|0305|05" | "0301|0305|06" | "0301|0305|07" |
             "0301|0305|08" | "0301|0305|09" | "0301|0305|10" | "0301|0305|11" | "0301|0305|12" |
             "0301|0305|13" | "0301|0305|14" | "0301|0305|15" | "0301|0305|17" | "0301|0305|18" |
             "0301|0305|19" |
             "0301|0306|04" | "0301|0306|05"
          => DATA(arr(1)+"\t"+arr(2),p)
        case "0301|0305|20" | "0301|0305|21" | "0301|0305|22" | "0301|0306|02" | "0301|0306|03" |
             "0301|1111|05" | "0301|1112|05"
          => DATA(arr(1)+"\t"+arr(3),p)
        case "0301|1111|01" | "0301|1111|02" | "0301|1111|03" |
             "0301|1112|01" | "0301|1112|02" | "0301|1112|03"
          => DATA(arr(1)+"\t"+arr(4),p)
        case "0301|0204|02" | "0301|0508|01" | "0301|1111|04" | "0301|1112|04"
          => DATA(arr(1)+"\t"+arr(5),p)
        case "0301|0204|04" => DATA(arr(1)+"\t"+arr(6),p)
        case "0301|0306|01" => DATA(arr(1)+"\t"+arr(8),p)
        case "0301|1010|01" | "0301|1010|02" | "0301|1010|03" | "0301|1010|05" | "0301|1010|06" |
             "0301|1010|07" | "0301|1010|08" | "0301|1010|09" | "0301|1010|10" | "0301|1010|11"
          => DATA(arr(2)+"\t"+arr(1),p)
        case _ => DATA(0+"\t"+0,p)
      }
    }).filter(p => {
      val arr = p.key.split("\t")
      arr.length == 2 && arr(0) != "" && arr(0) != "0" && arr(1) != "" && arr(1) != "0"
    }).sortBy(_.key).map(p => p.key + "\t" + p.value)
      .repartition(args(2).toInt).saveAsTextFile(args(1), classOf[GzipCodec])
  }
}
