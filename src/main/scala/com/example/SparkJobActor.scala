package com.example

import akka.actor.{Actor, ActorLogging, ActorRef}
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

/**
  * Created by neilri on 04/01/2017.
  */
class SparkJobActor extends Actor with ActorLogging {
  import SparkJobActor._

  def receive: Receive = {
    case Launch(appResource, mainClass, master, conf) => {
      val launcher = new SparkLauncher()
        .setAppResource(appResource)
        .setMainClass(mainClass)
        .setMaster(master)
      for ((key, value) <- conf.toList) {
        launcher.setConf(key, value)
      }

      val listener = new SparkAppHandle.Listener {
        override def infoChanged(handle: SparkAppHandle): Unit = {}
        override def stateChanged(handle: SparkAppHandle): Unit = self ! StateChanged
      }

      val handle = launcher.startApplication(listener)
      context become launched(handle)
    }
  }

  def launched(handle: SparkAppHandle): Receive = {
    case StateChanged => {
      import SparkAppHandle.State
      if (handle.getState.isFinal) {
        
      }
    }
  }
}

object SparkJobActor {
  case class Launch(appResource: String, mainClass: String, master: String, conf: Map[String, String])
  case object StateChanged
}