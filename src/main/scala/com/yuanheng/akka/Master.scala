package com.yuanheng.akka

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class Master  extends Actor{

  override def preStart(): Unit = {
    println("Master start")
  }


  override def receive: Receive = {
    case "connect"=>{
      println("a client connect")
      sender!"success"
    }

  }
}

object Master{
  def main(args: Array[String]): Unit = {
    //master 的 port 端口
    //准备配置文件信息
    val configStr=
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "localhost"
         |akka.remote.netty.tcp.port = "4444"
      """.stripMargin
    //配置 config 对象 利用 ConfigFactory 解析配置文件，获取配置信息
    val config=ConfigFactory.parseString(configStr)
    // 1、创建 ActorSystem,它是整个进程中老大，它负责创建和监督 actor，它是单例对象
    val masterActorSystem = ActorSystem("masterActorSystem",config)
    // 2、通过 ActorSystem 来创建 master actor
    val masterActor: ActorRef = masterActorSystem.actorOf(Props(new
        Master),"masterActor")
  }
}
