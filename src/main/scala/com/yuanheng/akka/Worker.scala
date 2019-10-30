package com.yuanheng.akka

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class Worker extends Actor{
  println("Worker constructor invoked")
  //prestart 方法会在构造代码块之后被调用，并且只会被调用一次
  override def preStart(): Unit = {
    println("preStart method invoked")
    val master: ActorSelection =
      context.actorSelection("akka.tcp://masterActorSystem@localhost:4444/user/masterActor")
    //向 master 发送消息
    master ! "connect"
  }
  //receive 方法会在 prestart 方法执行后被调用，不断的接受消息
  override def receive: Receive = {
    case "connect" =>{
      println("a client connected")
    }
    case "success" =>{
      println("注册成功")
    }
  }
}

object Worker{
  def main(args: Array[String]): Unit = {
    //定义 worker 的 IP 地址
    //定义 worker 的端口
    //准备配置文件
    val configStr=
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "localhost"
         |akka.remote.netty.tcp.port = "4446"
      """.stripMargin
    //通过 configFactory 来解析配置信息
    val config=ConfigFactory.parseString(configStr)
    // 1、创建 ActorSystem，它是整个进程中的老大，它负责创建和监督 actor
    val workerActorSystem = ActorSystem("workerActorSystem",config)
    // 2、通过 actorSystem 来创建 worker actor
    val workerActor: ActorRef = workerActorSystem.actorOf(Props(new Worker),"workerActor")
    //向 worker actor 发送消息
    workerActor ! "connect"
  }
}
