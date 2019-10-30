package com.yuanheng

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

class RddTest extends Serializable {
  @transient
  var sc:SparkContext=null;
  var data:RDD[String]=null;
  var ipData:RDD[String]=null;

  @Before
  def before={
    @transient
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount")
      .setMaster("local[2]")
      sparkConf.set("spark.testing.memory", "2147480000")
    sc= new SparkContext(sparkConf)
    data = sc.textFile("./access.log")
    ipData = sc.textFile("./ip.txt")

  }

  @Test
  def test01={
    val rdd: RDD[Int] = sc.parallelize(List(4,5,6,7))
    val result: Int = rdd.reduce(_+_)
    println(result)
  }
  @Test
  def test02={
    val rddArray: RDD[Array[String]] = data.map(_.split(" ")).filter(_.length>10)
    rddArray.filter(x=>{
      !x(10).equals("\"-\"")
    }).take(5).foreach(x=>{
      println(x(0)+",|"+x(1)+",|"+x(2)
        +",|"+x(3)+",|"+x(4)+",|"+x(5)
        +",|"+x(6)+",|"+x(7)+",|"+x(8)+",|"+x(9)+",|"+x(10))
    })
    val lineRdd: RDD[(String, Int)] = data.map(_.split(" ")).filter(_.length>10).map(x=>(x(10),1)).filter(_._1.length>5)
    val top10: Array[(String, Int)] = lineRdd.reduceByKey(_+_).sortBy(_._2,false).take(10)
    top10.foreach(x=>println(x))

  }

  /**
    * pv
    */
  @Test
  def test03={
    data.map(x=>("pv",1)).reduceByKey(_+_).foreach(x=>println(x))
    sc.stop()
  }

  /**
    * uv
    */
  @Test
  def test04={
    data.map(_.split(" ")).map(x=>x(0)).distinct().map(x=>("UV",1)).reduceByKey(_+_)
      .foreach(println)
    sc.stop()
  }

  @Test
  def test05={
    val jizhanRDD: RDD[(String, String, String, String, String)] = ipData.map(_.split("\\|")).map(
      x => (x(2), x(3), x(4) + "-" + x(5) + "-" + x(6) + "-" + x(7) + "-" + x(8), x(13), x(14)))

    //todo:获取RDD的数据
    val jizhanData: Array[(String, String, String, String, String)] = jizhanRDD.collect()
    val jizhanDataChe: Broadcast[Array[(String, String, String, String, String)]] = sc.broadcast(jizhanData)
    val ipDataResult: RDD[String] = sc.textFile("./20090121000132.394251.http.format").map(_.split("\\|")).map(x=>(x(1)))
    val result: RDD[((String, String), Int)] = ipDataResult.mapPartitions(item => {
      val jizhanValue: Array[(String, String, String, String, String)] = jizhanDataChe.value
      item.map(ip => {
        val num = ipToLong(ip)
        val index: Int = binarySearch(num, jizhanValue)
        val tuple = jizhanValue(index)
        ((tuple._4, tuple._5), 1)
      })
    })
    val resultFinal: RDD[((String, String), Int)] = result.reduceByKey(_+_)

    //todo:打印输出
    resultFinal.foreach(println)


    resultFinal.map(x=>(x._1._1,x._1._2,x._2)).repartition(1).sortBy(_._3).foreach(x=>println(x))
    sc.stop()
  }

  @Test
  def test06={
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount")
      .setMaster("local[2]")
    sparkConf.set("spark.testing.memory", "2147480000")
    val sc= new SparkContext(sparkConf)
    val a = sc.parallelize(1 to 10, 3)
    //定义两个输入变换函数，它们的作用均是将rdd a中的元素值翻倍
    //map的输入函数，其参数e为rdd元素值
    def myfuncPerElement(e:Int):Int = {
      println("e="+e)
      e*2
    }
    //mapPartitions的输入函数。iter是分区中元素的迭代子，返回类型也要是迭代子
    def myfuncPerPartition ( iter : Iterator [Int] ) : Iterator [Int] = {
      println("run in partition")
      var res = for (e <- iter ) yield e*2
      res
    }

    val b = a.map(myfuncPerElement).collect
    val c =  a.mapPartitions(myfuncPerPartition).collect

  }

  def binarySearch(ipNum: Long, valueArr: Array[(String, String, String, String, String)]): Int ={
    //todo:口诀：上下循环寻上下，左移右移寻中间
    //开始下标
    var start=0
    //结束下标
    var end=valueArr.length-1

    while(start<=end){
      val middle=(start+end)/2
      if(ipNum>=valueArr(middle)._1.toLong && ipNum<=valueArr(middle)._2.toLong){
        return middle
      }

      if(ipNum > valueArr(middle)._2.toLong){
        start=middle
      }

      if(ipNum<valueArr(middle)._1.toLong){
        end=middle
      }
    }

    -1
  }


  def ipToLong(ip: String): Long = {
    //todo：切分ip地址。
    val ipArray: Array[String] = ip.split("\\.")
    var ipNum=0L

    for(i <- ipArray){
      ipNum=i.toLong | ipNum << 8L
    }
    ipNum
  }

}
