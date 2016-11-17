package com.citic.guoan.dvb.live

import scala.io.Source
import scala.util.Sorting

/**
  * Created by liekkas on 16/11/17.
  */
object SortChannels {

  def main(args: Array[String]): Unit = {
    val channels = Source.fromFile("/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/live/channel_dict.txt")
      .getLines().toList
    channels.sorted.distinct.foreach(println(_))
  }
}
