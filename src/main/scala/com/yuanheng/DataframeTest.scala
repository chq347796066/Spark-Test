package com.yuanheng

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test


case class Person(id:Int,name:String,age:Int)
class DataframeTest {



  @Test
  def test01={
    val spark:SparkSession= SparkSession.builder().appName("SparksqlSchema")
        .config("spark.testing.memory","2147480000")
      .master("local[2]")
      .getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val personRdd: RDD[String] = sc.textFile("./person.txt")
    val personRow: RDD[Person] = personRdd.map(_.split(",")).map(x=>Person(x(0).toInt,x(1),x(2).toInt))
    val personDf: DataFrame = personRow.toDF
    personDf.show()
  }

  @Test
  def test02={
    val spark:SparkSession= SparkSession.builder().appName("SparksqlSchema")
        .config("spark.testing.memory","2147480000")
      .master("local[2]")
      .getOrCreate()
    val sc=spark.sparkContext
   val personDf: DataFrame = spark.read.text("./person.txt")
    personDf.show()
  }




}

