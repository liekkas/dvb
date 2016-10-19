package com.citic.guoan.dvb

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by liekkas on 16/10/14.
  */
object TestRedis {

  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("localhost")
    jedis.sadd("lan","scala")
    val result = jedis.smembers("lan")
    jedis.sadd("SUMMARY_D_MINI", 2.222+"\t"+3.333+"\t"+4.444+"\t"+"中文频道")
    print(result)
  }
}
