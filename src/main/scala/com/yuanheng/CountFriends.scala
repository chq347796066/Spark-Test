package com.yuanheng

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CountFriends {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]").set("spark.testing.memory","2147480000")
    val sc: SparkContext = new SparkContext(sparkConf)
    val file: RDD[String] = sc.textFile("./friend.txt")
    val rdd: RDD[(String, Array[String])] = file.map(x => {
      var key = x.split(":")(0)
      var value = x.split(":")(1)
      var array = value.split(",")
      (key, array)
    })
    val rdd1: RDD[((String, Array[String]), (String, Array[String]))] = rdd.cartesian(rdd)
    val rdd2=rdd1.filter(x=>{
      x._1._1!=x._2._1
    })
    val rdd3=rdd2.filter(x=>{
      val arr1=x._1._2
      val arr2=x._2._2
      var b=false
      for(a1<-arr1){
        for(a2<-arr2){
          if(a1.equals(a2)){
            b=true
          }
        }
      }
      b
    })
    

  }

}
