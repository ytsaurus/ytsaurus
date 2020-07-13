package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.discovery.DiscoveryService
import ru.yandex.spark.launcher.ByopLauncher.ByopConfig
import ru.yandex.spark.yt.wrapper.Utils
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

object WorkerLauncher extends App with VanillaLauncher with SparkLauncher with ByopLauncher {
  private val log = Logger.getLogger(getClass)
  private val workerArgs = WorkerLauncherArgs(args)
  private val byopConfig = ByopConfig.create(sparkSystemProperties, args)

  import workerArgs._

  prepareProfiler()

  withDiscovery(ytConfig, discoveryPath) { discoveryService =>
    log.info("Waiting for master http address")
    val masterAddress = discoveryService.waitAddress(waitMasterTimeout)
      .getOrElse(throw new IllegalStateException(s"Empty discovery path $discoveryPath, master is not started"))

    withOptionService(byopConfig.map(startByop(_, ytConfig, waitMasterTimeout))) { byop =>
      log.info(s"Starting worker for master $masterAddress")
      withService(startWorker(masterAddress, cores, memory)) {worker =>
        def isAlive: Boolean = {
          val isMasterAlive = DiscoveryService.isAlive(masterAddress.hostAndPort, 3)
          val isWorkerAlive = worker.isAlive(3)
          val isRpcProxyAlive = byop.forall(_.isAlive(3))

          if (!isMasterAlive) log.error("Master is not alive")
          if (!isWorkerAlive) log.error("Worker is not alive")
          if (!isRpcProxyAlive) log.error("Rpc proxy is not alive")

          isMasterAlive && isWorkerAlive && isRpcProxyAlive
        }

        checkPeriodically(isAlive)
      }
    }
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
    args.optional("wait-master-timeout").map(Utils.parseDuration).getOrElse(5 minutes)
  )

  def apply(args: Array[String]): WorkerLauncherArgs = WorkerLauncherArgs(Args(args))
}
