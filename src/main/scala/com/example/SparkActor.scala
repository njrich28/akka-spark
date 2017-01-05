package com.example

import akka.actor.{Actor, ActorLogging, Cancellable, Props}

import scala.concurrent.blocking
import scala.concurrent.duration._

/**
  * Created by neilri on 21/12/2016.
  */
class SparkActor extends Actor with ActorLogging {
  import SparkActor._

  val scheduler = context.system.scheduler

  implicit val ec = context.dispatcher

  val pollDelay = 2.seconds

  def receive = {
    case Run(id) => {
      log.info("Running " + id)
      val process = runProcess
      val poller = scheduler.schedule(pollDelay, pollDelay, self, IsAlive)
      context become running(id, process, poller)
    }
  }

  def running(id: Integer, process: Process, poller: Cancellable): Receive = {
    case IsAlive => {
      if (process.isAlive) {
        log.info(s"Process $id still alive")
      } else {
        log.info(s"Process $id finished")
        poller.cancel()
        context.parent ! Finished(id)
        context.stop(self)
      }
    }
  }

}

class SlightlyFixedSparkActor extends Actor with ActorLogging {
  import SparkActor._

  def receive = {
    case Run(id) => {
      log.info("Running " + id)
      val p = runProcess
      blocking {
        p.waitFor()
      }
      log.info(s"Process $id finished")
      sender ! Finished(id)
    }
  }
}

class BrokenSparkActor extends Actor with ActorLogging {
  import SparkActor._

  def receive = {
    case Run(id) => {
      log.info("Running " + id)
      val p = runProcess
      p.waitFor()
      log.info(s"Process $id finished")
      sender ! Finished(id)
    }
  }
}

object SparkActor {
  val props = Props[SparkActor]
  val propsSlightlyFixed = Props[SlightlyFixedSparkActor]
  val propsBroken = Props[BrokenSparkActor]

  def runProcess = new ProcessBuilder("sleep", "10s").start()

  case class Run(id: Integer)
  case class Finished(id: Integer)
  case object IsAlive
}