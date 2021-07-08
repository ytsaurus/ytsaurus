package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import ru.yandex.spark.launcher.ByopLauncher.ByopConfig
import ru.yandex.spark.yt.wrapper.Utils.parseDuration
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration
import ru.yandex.spark.yt.wrapper.discovery.DiscoveryService

import scala.concurrent.duration._
import scala.language.postfixOps

object WorkerLauncher extends App with VanillaLauncher with SparkLauncher with ByopLauncher with SolomonLauncher {
  private val log = LoggerFactory.getLogger(getClass)
  private val workerArgs = WorkerLauncherArgs(args)
  private val byopConfig = ByopConfig.create(sparkSystemProperties, args)

  import workerArgs._

  prepareProfiler()

  withOptionService(byopConfig.map(startByop)) { byop =>
    withDiscovery(ytConfig, discoveryPath) { case (discoveryService, _) =>
      log.info("Waiting for master http address")
      val masterAddress = discoveryService.waitAddress(waitMasterTimeout)
        .getOrElse(throw new IllegalStateException(s"Empty discovery path $discoveryPath, master is not started"))

      log.info(s"Starting worker for master $masterAddress")
      withService(startWorker(masterAddress, cores, memory)) { worker =>
        withService(startSolomonAgent(args, "worker", worker.address.getPort)) { solomonAgent =>
          def isAlive: Boolean = {
            val isMasterAlive = DiscoveryService.isAlive(masterAddress.hostAndPort, 3)
            val isWorkerAlive = worker.isAlive(3)
            val isRpcProxyAlive = byop.forall(_.isAlive(3))
            val isSolomonAgentAlive = solomonAgent.isAlive(3)

            isMasterAlive && isWorkerAlive && isRpcProxyAlive && isSolomonAgentAlive
          }

          checkPeriodically(isAlive)
        }
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
    args.optional("wait-master-timeout").map(parseDuration).getOrElse(5 minutes)
  )

  def apply(args: Array[String]): WorkerLauncherArgs = WorkerLauncherArgs(Args(args))
}
