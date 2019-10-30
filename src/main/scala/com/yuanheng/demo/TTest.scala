package com.yuanheng.demo

trait TTest {
  type T
  def transform(x:T):T
  val initial:T
  var current:T

}
