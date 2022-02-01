package ru.yandex.spark.yt.test

import com.whisk.docker.{ContainerLink, DockerContainer, DockerReadyChecker}
import org.scalatest.{BeforeAndAfterAll, TestSuite}
import ru.yandex.spark.yt.wrapper.client._
import ru.yandex.yt.ytclient.proxy.CompoundClient

import scala.concurrent.duration._
import scala.language.postfixOps

trait LocalYt extends BeforeAndAfterAll {
  self: TestSuite =>

  System.setProperty("io.netty.tryReflectionSetAccessible", "true")

  protected def ytRpcClient: YtRpcClient

  protected implicit lazy val yt: CompoundClient = ytRpcClient.yt

  LocalYt.init()
}

object LocalYt extends DockerCompose {
  val host: String = sys.env.getOrElse("YT_LOCAL_HOST", "localhost")
  val proxyPort = 8000
  val rpcProxyPort = 8002
  private val innerProxyPort = 80
  private val ytDockerName = "ytbackend"
  private val ytInterfaceDockerName = "ytfrontend"
  override protected def networkName: String = "YT"

  override protected def shutdownHook(): Unit = {
    super.shutdownHook()
    YtClientProvider.close()
  }

  override protected def cleanPrevEnvironment(): Unit = {
    Seq(ytDockerName, ytInterfaceDockerName).par.foreach(stopAndRmContainer)
    super.cleanPrevEnvironment()
  }

  lazy val ytDocker = DockerContainer("registry.yandex.net/yt/yt:sashbel31012022", name = Some(ytDockerName))
    .withPorts(innerProxyPort -> Some(proxyPort), rpcProxyPort -> Some(rpcProxyPort))
    .withReadyChecker(
      DockerReadyChecker.And(
        DockerReadyChecker
          .HttpResponseCode(innerProxyPort, path = "/hosts", host = Some(host))
          .looped(100, 5 seconds)
          .within(10 minutes),
        DockerReadyChecker
          .LogLineContains("Local YT started")
          .looped(100, 5 seconds)
          .within(10 minutes)
      )
    )
    .withEntrypoint(
      "bash", "/usr/bin/start-patched.sh",
      host,
      "--proxy-config",
      s"""{address_resolver={enable_ipv4=%true;enable_ipv6=%false;};coordinator={public_fqdn=\"localhost:$proxyPort\"}}""",
      "--rpc-proxy-count", "1",
      "--rpc-proxy-port", rpcProxyPort.toString
    )

  lazy val ytInterfaceDocker = DockerContainer("registry.yandex.net/yt/yt-interface:stable", name = Some(ytInterfaceDockerName))
    .withPorts(80 -> Some(8001))
    .withEnv(
      s"PROXY=localhost:$proxyPort",
      "APP_ENV=local",
      s"PROXY_INTERNAL=$ytDockerName",
      "UI_CORE_CDN=false",
      "DENY_DISPENSER_PROXY=1",
      "DENY_YQL_PROXY=1",
      "RUM_ENV=local"
    )
    .withReadyChecker(
      DockerReadyChecker.Always
    )
    .withLinks(ContainerLink(ytDocker, ytDockerName))

  override def dockerContainers: List[DockerContainer] = {
    List(ytDocker, ytInterfaceDocker)
      .map(_.withNetworkMode(networkName))
      .filter(container => !reuseDocker || !isRunning(container.name.get)) ++ super.dockerContainers
  }
}

trait LocalYtClient extends LocalYt {
  self: TestSuite =>

  private val conf: YtClientConfiguration = YtClientConfiguration(
    proxy = s"${LocalYt.host}:${LocalYt.proxyPort}",
    user = "root",
    token = "",
    timeout = 5 minutes,
    proxyRole = None,
    byop = ByopConfiguration(
      enabled = false,
      ByopRemoteConfiguration(enabled = false, EmptyWorkersListStrategy.Default)
    ),
    masterWrapperUrl = None,
    extendedFileTimeout = true
  )

  override protected def ytRpcClient: YtRpcClient = {
    YtClientProvider.ytRpcClient(conf)
  }
}
