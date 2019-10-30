package com.yuanheng.sql

import com.yuanheng.utils.SparkSessionUtils
import org.apache.spark.sql.SparkSession

object SparkSqlTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionUtils.getSparkSession()

  }


}
