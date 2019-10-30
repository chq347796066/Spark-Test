package com.yuanheng.demo

class Email {
  var name=""
  var domain=""
  def this(name:String,domain:String)={
    this()
    this.name=name
    this.domain=domain
  }

}

object Email{
  def apply(name:String,domain:String): Email = new Email(name,domain)
}
