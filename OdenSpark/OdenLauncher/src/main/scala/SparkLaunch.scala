import java.util.concurrent.CountDownLatch

import akka.actor.{Actor, ActorSystem}
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.slf4j.{Logger, LoggerFactory}


// Actor to Launch SparkOden
class SparkLaunch extends Actor {

  val logger: Logger = LoggerFactory.getLogger("SparkLaunch")
  val system: ActorSystem = ActorSystem("SparkOden")
  val countDownLatch = new CountDownLatch(1)

  val launcher: SparkLauncher = new SparkLauncher()
    .setSparkHome("/opt/spark-3.0.0")
    .setAppResource("/home/bw/scala/Spark/Oden/build/libs/Oden.jar")
    .setMainClass("Main")
    .setMaster("local[*]")
    .setVerbose(false)
    .redirectError()
    .redirectToLog("console")

  def receive: Receive = {

    case Launch =>

      val listener = new SparkAppHandle.Listener {
        override def infoChanged(handle: SparkAppHandle): Unit = {}
        override def stateChanged(handle: SparkAppHandle): Unit = {
          logger.warn(s"Spark App Id [${handle.getAppId}] State Changed. State [${handle.getState}]")
          if (handle.getState.isFinal && handle.getAppId != null) {
            logger.warn(s"Spark App Id [${handle.getAppId}] state isFinal...")
            countDownLatch.countDown()
          }
        }
      }
      val handle: SparkAppHandle = launcher.startApplication(listener)
      countDownLatch.await()
      handle.kill()
      system.terminate()
  }

}

object Launch