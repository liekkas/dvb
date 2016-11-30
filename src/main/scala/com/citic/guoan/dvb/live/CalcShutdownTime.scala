package com.citic.guoan.dvb.live

import com.citic.guoan.dvb.partitioner.ViewerIdPartitioner
import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.Nil
import scala.util.Random

/**
  * Created by liekkas on 16/10/26.
  * 计算关机时间
  * 开机时间的上一个心跳时间 + 一个心跳周期内的随机时间 当做关机时间,并生成一条关机记录
  */
object CalcShutdownTime {
  case class LIVE(uid:String,date:String,openMark:Int,shutTimes: List[String])
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CalcShutdownTime")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LIVE]))
    val sc = new SparkContext(conf)

    def filterFunc(line: Array[String]): Boolean = {
      (line(0) == HEARTBEAT_MARKER || line(0) == OPEN_MARKER) &&
        line(1) != "" && line(2) != "" &&
        line(1).length == 14 && line(2).length > 6 &&
        line(1).substring(0,6).toInt > 201603 &&
        line(1).substring(0,6).toInt < 201610
    }

    val partitioner = new ViewerIdPartitioner(args(2).toInt)
    sc.textFile(args(0))
      .map(_.split(";")) //分解出地区|采集系统ID|内容
      .filter(filterFunc)
      .map(p => {
        val openMark = if(p(0) == OPEN_MARKER) 1 else 0
        (p(2)+","+p(1),LIVE(p(2),p(1), openMark, Nil))
      })
      .repartitionAndSortWithinPartitions(partitioner) //重新分区并按UID和时间排序
      .map(p => (p._2.uid,p._2))
      .reduceByKey(partitioner,(current,next) => {
        //如果下条信息是开机信息,那说明本条信息是最后心跳时间,
        // 由于心跳时间是十分钟,所以还是不能确定最终的具体时间,但肯定在最后一条心跳时间之后的十分钟内
        // 所以这里用随机数来处理,在最后的心跳时间上加个(十分钟内)的随机时间
        if(next.openMark == 1) {
          val lastDate = DateUtils.parseDate(current.date,DATE_FORMAT_STR)
          val date = DateUtils.addMinutes(lastDate,Random.nextInt(10))
          val dateStr = DateFormatUtils.format(date,DATE_FORMAT_STR)
          val shutTimes = current.shutTimes :+ dateStr
          LIVE(next.uid,next.date,2,shutTimes)
        } else {
          LIVE(next.uid,next.date,2,current.shutTimes)
        }
      })
      .map(f => {
        val shutTimes = f._2.shutTimes
        var result = ""
        //防止偶然的不换行现象,强制每条数据都加上换行符,用两下划线表示这是关机信息,和正常的直播信息一致,便于计算时长
        shutTimes.foreach(time => {
          result += f._2.uid + "\t" + time + "\t" + "_" + "\t" + "_" + "\n"
        })
        result
      })
      .filter(_ != "") //过滤掉空串,但注意换行符过滤不掉 后面计算任务都需要先再过滤一遍
      .saveAsTextFile(args(1),classOf[GzipCodec])
//      .saveAsTextFile(args(1))
  }
}
