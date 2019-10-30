package com.yuanheng.utils

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextUtils {
  val apName="spark";
  def getSc(apName:String):SparkContext={
    val sparkConf = new SparkConf().setAppName(apName).setMaster("local[2]").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("./flume")
    sc
  }
  def getSc():SparkContext={
    val sparkConf = new SparkConf().setAppName(apName).setMaster("local[2]").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("./flume")
    sc
  }

}
