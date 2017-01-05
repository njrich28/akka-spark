package com.example

import akka.actor.{Actor, ActorLogging, Props}

/**
  * Created by neilri on 21/12/2016.
  */
class SupervisionActor extends Actor with ActorLogging {

  val numChildren = 80

  var countFinished = 0

  def receive = {
    case SupervisionActor.RunJob => {
      for ( id <- 1 to numChildren ) {
        val sparkActor = context.actorOf(SparkActor.propsBroken)
        sparkActor ! SparkActor.Run(id)
      }
    }
    case SparkActor.Finished(id) => {
      log.info("Supervisor notified finished " + id)
      countFinished += 1
      if (countFinished == numChildren) context.system.shutdown()
    }
  }
}

object SupervisionActor {
  val props = Props[SupervisionActor]

  case object RunJob
}