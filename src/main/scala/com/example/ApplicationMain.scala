package com.example

import akka.actor.ActorSystem

object ApplicationMain extends App {
  val system = ActorSystem("MyActorSystem")
  val supervisionActor = system.actorOf(SupervisionActor.props, "supervisionActor")
  supervisionActor ! SupervisionActor.RunJob
  // This example app will ping pong 3 times and thereafter terminate the ActorSystem - 
  // see counter logic in PingActor
  system.awaitTermination()
}