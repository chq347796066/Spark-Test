package com.yuanheng.demo

import org.junit.Test

import scala.collection.immutable.TreeMap
import scala.collection.mutable

class Demo01 {
  @Test
  def test01={
    val map=mutable.Map(("key01","key01"),("key02","key02"))
    println(map)

  }
  @Test
  def test02={
    val set=mutable.TreeSet(1,5,9,19,20,8,2)
    println(set)
    val map=TreeMap((2,"test"),(9,"test"),(3,"test"),(4,"test"))
    println(map)


  }
  @Test
  def test03={
    val muta=Map((1,"1"),(2,"2"))
    val immu=Map.empty++muta
    println(immu)


  }
 

}

object Demo01{
  def main(args: Array[String]): Unit = {
    println("this is a test")
  }
}
