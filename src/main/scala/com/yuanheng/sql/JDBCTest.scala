package com.yuanheng.sql

import com.yuanheng.utils.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import org.apache.spark.sql.expressions._

object JDBCTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionUtils.getSparkSession()
    val studentDf: DataFrame = spark.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3306/sql_learn", "dbtable" -> "student")).load()

  }

}
