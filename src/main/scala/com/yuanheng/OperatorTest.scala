package com.yuanheng

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}


class Count{

}
class OperatorTest {
  var sc:SparkContext=null;
  @Before
  def before={
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount")
      .setMaster("local[2]")
    sparkConf.set("spark.testing.memory", "2147480000")
    sparkConf.registerKryoClasses(Array(classOf[Count]))
    sc= new SparkContext(sparkConf)

  }
  @Test
  def test01={
    val data: RDD[String] = sc.textFile("./test.txt",5)
    val rddArray: RDD[Array[String]] = data.map(_.split(" "))
    rddArray.foreach(x=>{
      for(b<-x){
        println(b)
        println("==")
      }
    })
    println("============================================")
    data.flatMap(_.split(" ")).foreach(x=>println(x))


  }
  @Test
  def test02={
    val a: RDD[(Int, Int)] = sc.parallelize(List((1, 2), (3, 4), (5, 6)))
    val b = a.flatMapValues(x => 1.to(x))
    b.collect.foreach(println)
  }
  @Test
  def test03={
    val a=("key","value")
    val b=("key","value")
    println(a.eq(b))
    val c="key"+"value"
    val d="key"+"value"
    println(c.eq(d))

  }

}
