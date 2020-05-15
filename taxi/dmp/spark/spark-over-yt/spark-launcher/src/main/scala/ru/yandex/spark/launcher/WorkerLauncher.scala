package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.discovery.DiscoveryService
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

object WorkerLauncher extends App with VanillaLauncher with SparkLauncher with RpcProxyLauncher {
  private val log = Logger.getLogger(getClass)
  private val workerArgs = WorkerLauncherArgs(args)
  private val rpcProxyConfig = RpcProxyConfig.create(sparkSystemProperties, args)

  run(workerArgs.ytConfig, workerArgs.discoveryPath) { discoveryService =>
    log.info("Waiting for master http address")
    val masterAddress = discoveryService.waitAddress(workerArgs.waitMasterTimeout)
      .getOrElse(throw new IllegalStateException(s"Empty discovery path ${workerArgs.discoveryPath}, master is not started"))

    val rpcProxyThread = rpcProxyConfig.map(startRpcProxy(workerArgs.ytConfig, _))

    Try {
      val rpcProxyAddress = rpcProxyConfig.map(waitRpcProxyStart(_, workerArgs.waitMasterTimeout))

      log.info(s"Starting worker for master $masterAddress")
      val workerThread = startWorker(masterAddress, workerArgs.cores, workerArgs.memory)

      Try {
        def isAlive: Boolean = {
          val isMasterAlive = DiscoveryService.isAlive(masterAddress.webUiHostAndPort, 3)
          val isWorkerAlive = workerThread.isAlive
          val isRpcProxyAlive = rpcProxyThread.forall(_.isAlive) &&
            rpcProxyAddress.forall(DiscoveryService.isAlive(_, 3))

          if (!isMasterAlive) log.error("Master is not alive")
          if (!isWorkerAlive) log.error("Worker is not alive")
          if (!isRpcProxyAlive) log.error("Rpc proxy is not alive")

          isMasterAlive && isWorkerAlive && isRpcProxyAlive
        }

        checkPeriodically(isAlive)
      }

      log.info("Interrupt worker thread")
      workerThread.interrupt()
    }

    log.info("Interrupt rpc proxy thread")
    rpcProxyThread.foreach(_.interrupt())
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

