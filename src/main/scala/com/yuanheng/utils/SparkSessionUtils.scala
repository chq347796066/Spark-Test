package com.yuanheng.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkSessionUtils {
  def getSparkSession():SparkSession={
    val sparkConf = new SparkConf()
      .setAppName("PV1")
      .setMaster("local[2]")
      .set("spark.testing.memory", "2147480000")
    val spark: SparkSession = SparkSession.builder()
        .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    spark
  }

}
