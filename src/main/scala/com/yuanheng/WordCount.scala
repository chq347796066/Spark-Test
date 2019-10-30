package com.yuanheng

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)
    val file: RDD[String] = sc.textFile(args(0))
    val words: RDD[String] = file.flatMap(_.split(" "))
    val wordAndOne: RDD[(String, Int)] = words.map(x=>(x,1))
    val result: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    result.saveAsTextFile(args(1))
    sc.stop()
  }

}
