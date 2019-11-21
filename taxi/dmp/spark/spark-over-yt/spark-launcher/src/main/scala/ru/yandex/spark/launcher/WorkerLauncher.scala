package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.discovery.{CypressDiscoveryService, DiscoveryService}
import ru.yandex.spark.yt.utils.YtClientConfiguration

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps

object WorkerLauncher extends App {
  val log = Logger.getLogger(getClass)
  val workerArgs = WorkerLauncherArgs(args)

  val discoveryService = new CypressDiscoveryService(workerArgs.ytConfig, workerArgs.discoveryPath)

  try {
    log.info("Waiting for master http address")
    val masterAddress = discoveryService.waitAddress(workerArgs.id, 5 minutes)
      .getOrElse(throw new IllegalStateException(s"Unknown master id: ${workerArgs.id}"))

    log.info(s"Starting worker for master $masterAddress")
    log.info(s"Worker opts: ${workerArgs.opts}")
    log.info(s"Worker args: ${args.mkString(" ")}")
    val thread = SparkLauncher.startWorker(masterAddress, workerArgs.port, workerArgs.webUiPort,
      workerArgs.cores, workerArgs.memory, workerArgs.opts)
    def masterIsAlive: Boolean = DiscoveryService.isAlive(masterAddress.webUiHostAndPort)
    DiscoveryService.checkPeriodically(thread.isAlive && masterIsAlive)
    log.warn(s"Worker is alive: ${thread.isAlive}, master is alive: $masterIsAlive")
  } finally {
    discoveryService.close()
    SparkLauncher.stopSlave()
  }
}

case class WorkerLauncherArgs(id: String,
                              port: Option[Int],
                              webUiPort: Int,
                              cores: Int,
                              memory: String,
                              opts: Option[String],
                              ytConfig: YtClientConfiguration,
                              discoveryPath: String)

object WorkerLauncherArgs {
  def apply(args: Args): WorkerLauncherArgs = WorkerLauncherArgs(
    args.required("id"),
    args.optional("port").map(_.toInt),
    args.optional("web-ui-port").map(_.toInt).getOrElse(8081),
    args.required("cores").toInt,
    args.required("memory"),
    args.optional("opts").map(_.drop(1).dropRight(1)),
    YtClientConfiguration(args.optional),
    args.optional("discovery-path").getOrElse(sys.env("SPARK_DISCOVERY_PATH")),
  )

  def apply(args: Array[String]): WorkerLauncherArgs = WorkerLauncherArgs(Args(args))
}

