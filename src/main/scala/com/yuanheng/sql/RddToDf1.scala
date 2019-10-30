package com.yuanheng.sql

import com.yuanheng.utils.{SparkContextUtils, SparkSessionUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RddToDf1 {
  case class User(id:String,name:String,age:String,sex:String)
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtils.getSc()
    val spark: SparkSession = SparkSessionUtils.getSparkSession()
    val fileRdd: RDD[String] = sc.textFile("E:\\weblog\\user.txt")
    val rowRdd: RDD[Row] = fileRdd.map(x=>x.split(",")).map(x=>Row(x(0),x(1),x(2),x(3)))
    val fieldArray = Array(StructField("id", StringType, true), StructField("name", StringType, true),
      StructField("age", StringType, true), StructField("sex", StringType, true))
    val structType = StructType(fieldArray)
    val resultDf: DataFrame = spark.createDataFrame(rowRdd,structType)
    resultDf.show()
  }

}
