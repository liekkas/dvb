package com.citic.guoan.dvb.live.prepare

import java.io.File

import org.apache.commons.io.FileUtils
import redis.clients.jedis.Jedis

import scala.util.Sorting

/**
  * Created by liekkas on 16/11/16.
  */
object SavePrepareSum {
  def main(args: Array[String]): Unit = {
    val resultFile = new File("/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/live/live_prepare_sum.txt")
    val jedis = new Jedis("z1")
    val result = jedis.smembers("RESULT").toArray.map(_.toString)
    Sorting.quickSort(result)
    result.foreach(row => {
      println(row)
      FileUtils.writeStringToFile(resultFile,row+"\n",true)
    })
  }
}
