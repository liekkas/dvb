package com.citic.guoan.dvb.partitioner

import org.apache.spark.Partitioner

/**
  * Created by liekkas on 16/11/10.
  * PartitionBy Viewer's Id
  */
class ViewerIdPartitioner(numParts: Int) extends Partitioner{
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    //由于原uid的虽然是数字,但长度不一致,而且普遍有17、8个字至多,所以取最后6个数字来
    val uidStr = key.toString.split(",")(0)
    val uid = if (uidStr.length <= 6) uidStr.toInt else uidStr.substring(uidStr.length-6).toInt
    uid % numPartitions
  }
}
