package com.citic.guoan.dvb

import java.util.Date

import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )

    println("2016-04-01" > "2016-03-31")
  }

}
