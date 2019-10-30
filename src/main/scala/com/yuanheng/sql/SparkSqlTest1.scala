package com.yuanheng.sql

import com.yuanheng.utils.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlTest1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionUtils.getSparkSession()
    val studentDf: DataFrame = spark.read.format("json").load("E:\\weblog\\student.json")
    studentDf.createOrReplaceTempView("student")
    val resultDf: DataFrame = spark.sql("select * from student")
    resultDf.write.saveAsTable("students")
    resultDf.show()



  }

}
