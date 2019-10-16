package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.discovery.model.CypressDiscoveryService
import ru.yandex.spark.yt.utils.YtClientConfiguration

import scala.concurrent.duration._
import scala.language.postfixOps

object WorkerLauncher extends App {
  val log = Logger.getLogger(getClass)
  val workerLauncherArgs = WorkerLauncherArgs(args)

  val discoveryService = new CypressDiscoveryService(workerLauncherArgs.ytConfig, workerLauncherArgs.discoveryPath)

  try {
    log.info("Waiting for master http address")
    val masterAddress = discoveryService.waitAddress(workerLauncherArgs.id, 5 minutes)
      .getOrElse(throw new IllegalStateException(s"Unknown master id: ${workerLauncherArgs.id}"))

    log.info(s"Starting worker for master $masterAddress")
    SparkLauncher.startSlave(masterAddress, workerLauncherArgs.cores, workerLauncherArgs.memory)
    discoveryService.checkPeriodically(masterAddress)
  } finally {
    discoveryService.close()
    SparkLauncher.stopSlave()
  }
}

case class WorkerLauncherArgs(id: String,
                              cores: Int,
                              memory: String,
                              ytConfig: YtClientConfiguration,
                              discoveryPath: String)

object WorkerLauncherArgs {
  def apply(args: Args): WorkerLauncherArgs = WorkerLauncherArgs(
    args.required("id"),
    args.required("cores").toInt,
    args.required("memory"),
    YtClientConfiguration(args.optional),
    args.optional("discovery-path").getOrElse(sys.env("SPARK_DISCOVERY_PATH")),
  )

  def apply(args: Array[String]): WorkerLauncherArgs = WorkerLauncherArgs(Args(args))
}

