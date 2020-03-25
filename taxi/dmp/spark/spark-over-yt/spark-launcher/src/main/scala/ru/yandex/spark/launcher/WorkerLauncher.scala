package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.discovery.DiscoveryService
import ru.yandex.spark.yt.utils.YtClientConfiguration

import scala.concurrent.duration._
import scala.language.postfixOps

object WorkerLauncher extends App with SparkLauncher {
  val log = Logger.getLogger(getClass)
  val workerArgs = WorkerLauncherArgs(args)

  run(workerArgs.ytConfig, workerArgs.discoveryPath) { discoveryService =>
    log.info("Waiting for master http address")
    val masterAddress = discoveryService.waitAddress(workerArgs.waitMasterTimeout)
      .getOrElse(throw new IllegalStateException(s"Empty discovery path ${workerArgs.discoveryPath}, master is not started"))

    log.info(s"Starting worker for master $masterAddress")
    startWorker(masterAddress, workerArgs.cores, workerArgs.memory)

    def masterIsAlive: Boolean = DiscoveryService.isAlive(masterAddress.webUiHostAndPort)

    checkPeriodically(sparkThreadIsAlive && masterIsAlive)
    log.warn(s"Worker is alive: $sparkThreadIsAlive, master is alive: $masterIsAlive")
  }
}

case class WorkerLauncherArgs(cores: Int,
                              memory: String,
                              ytConfig: YtClientConfiguration,
                              discoveryPath: String,
                              waitMasterTimeout: Duration)

object WorkerLauncherArgs {
  def apply(args: Args): WorkerLauncherArgs = WorkerLauncherArgs(
    args.required("cores").toInt,
    args.required("memory"),
    YtClientConfiguration(args.optional),
    args.optional("discovery-path").getOrElse(sys.env("SPARK_DISCOVERY_PATH")),
    args.optional("wait-master-timeout").map(parseDuration).getOrElse(5 minutes)
  )

  def apply(args: Array[String]): WorkerLauncherArgs = WorkerLauncherArgs(Args(args))

  private def parseDuration(s: String): Duration = {
    val regex = """(\d+)(.*)""".r
    s match {
      case regex(amount, "m") => amount.toInt.minutes
      case regex(amount, "s") => amount.toInt.seconds
      case regex(amount, "h") => amount.toInt.hours
    }
  }
}

