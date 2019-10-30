package com.yuanheng.sql

import com.yuanheng.utils.{SparkContextUtils, SparkSessionUtils}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RddToDf {
  case class User(id:String,name:String,age:String,sex:String)
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtils.getSc()
    val spark: SparkSession = SparkSessionUtils.getSparkSession()
    import spark.implicits._
    val fileRdd: RDD[String] = sc.textFile("E:\\weblog\\user.txt")
    val userRdd: RDD[User] = fileRdd.map(x=>x.split(",")).map(x=>User(x(0),x(1),x(2),x(3)))
    val resultDf: DataFrame = userRdd.toDF()
    resultDf.show()
  }

}
