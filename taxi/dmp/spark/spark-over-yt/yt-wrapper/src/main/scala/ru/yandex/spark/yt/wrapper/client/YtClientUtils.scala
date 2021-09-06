package ru.yandex.spark.yt.wrapper.client


import java.util.{ArrayList => JArrayList}
import com.google.common.net.HostAndPort
import io.netty.channel.nio.NioEventLoopGroup
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.spark.yt.wrapper.system.SystemUtils
import ru.yandex.yt.ytclient.bus.DefaultBusConnector
import ru.yandex.yt.ytclient.proxy.internal.{DiscoveryMethod, HostPort}
import ru.yandex.yt.ytclient.proxy.{CompoundClient, YtClient, YtCluster}
import ru.yandex.yt.ytclient.rpc.RpcOptions

import java.util.concurrent.ThreadFactory
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

trait YtClientUtils {
  private val log = LoggerFactory.getLogger(getClass)

  def createRpcClient(id: String, config: YtClientConfiguration): YtRpcClient = {
    log.info(s"Create RPC YT Client, id $id, configuration ${config.copy(token = "*****")}")

    createYtClient(id, config.timeout) { case (connector, options) => createYtClient(config, connector, options) }
  }

  private def createYtClient(id: String, timeout: Duration)
                            (client: (DefaultBusConnector, RpcOptions) => CompoundClient): YtRpcClient = {
    val daemonThreadFactory = new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r)
        thread.setDaemon(true)
        thread
      }
    }

    val group = new NioEventLoopGroup(1, daemonThreadFactory)
    val connector = new DefaultBusConnector(group, true)
      .setReadTimeout(toJavaDuration(timeout))
      .setWriteTimeout(toJavaDuration(timeout))

    try {
      val rpcOptions = new RpcOptions()
      rpcOptions.setTimeouts(timeout)
      rpcOptions.setNewDiscoveryServiceEnabled(true)
      rpcOptions.setDiscoveryThreadFactory(daemonThreadFactory)

      val yt = client(connector, rpcOptions)
      log.info(s"YtClient $id created")
      YtRpcClient(id, yt, connector)
    } catch {
      case e: Throwable =>
        connector.close()
        throw e
    }
  }

  private def createYtClient(config: YtClientConfiguration,
                             connector: DefaultBusConnector,
                             rpcOptions: RpcOptions): CompoundClient = {
    byopLocalEndpoint(config) match {
      case Some(byop) =>
        log.info(s"Create local BYOP client with config $byop")
        createByopLocalProxyClient(connector, rpcOptions, config, byop)
      case None =>
        byopRemoteEndpoint(config) match {
          case Some(byopRemote) =>
            log.info(s"Create remote BYOP client with config $byopRemote")
            createByopRemoteProxiesClient(connector, rpcOptions, config, byopRemote)
          case None =>
            log.info(s"Create remote proxies client")
            createRemoteProxiesClient(connector, rpcOptions, config)
        }
    }
  }

  private def byopLocalEndpoint(config: YtClientConfiguration): Option[HostAndPort] = {
    if (config.byop.enabled && SystemUtils.isEnabled("byop")) {
      for {
        host <- SystemUtils.envGet("byop_host")
        port <- SystemUtils.envGet("byop_port").map(_.toInt)
      } yield HostAndPort.fromParts(host, port)
    } else None
  }

  private def byopRemoteEndpoint(config: YtClientConfiguration): Option[HostAndPort] = {
    if (config.byop.remote.enabled) {
      config.masterWrapperUrl.map(HostAndPort.fromString) match {
        case Some(url) =>
          val client = new MasterWrapperClient(url)
          client.byopEnabled match {
            case Success(true) =>
              log.info("BYOP enabled in cluster")
              client.discoverProxies match {
                case Success(Nil) | Failure(_) =>
                  log.info(s"Workers list is empty, use strategy ${config.byop.remote.emptyWorkersListStrategy}")
                  config.byop.remote.emptyWorkersListStrategy.use(client)
                case Success(_) =>
                  log.info("Workers list non empty")
                  Some(url)
              }
            case Success(false) =>
              log.info("BYOP disabled in cluster")
              None
            case Failure(exception) =>
              log.warn(s"Unexpected error in BYOP Discovery: ${exception.getMessage}")
              None
          }
        case None =>
          log.info("BYOP enabled, but BYOP Discovery URL is not provided")
          None
      }
    } else None
  }

  private def createByopLocalProxyClient(connector: DefaultBusConnector,
                                         rpcOptions: RpcOptions,
                                         config: YtClientConfiguration,
                                         byopEndpoint: HostAndPort): SingleProxyYtClient = {
    new SingleProxyYtClient(
      connector,
      config.rpcCredentials,
      rpcOptions,
      HostPort.parse(byopEndpoint.toString)
    )
  }

  private def createRemoteProxiesClient(connector: DefaultBusConnector,
                                        rpcOptions: RpcOptions,
                                        config: YtClientConfiguration): YtClient = {
    val cluster = new YtCluster(
      config.shortProxy,
      config.fullProxy,
      config.port,
      new JArrayList(),
      toOptional(config.proxyRole))

    val client = new YtClient(
      connector,
      cluster,
      config.rpcCredentials,
      rpcOptions
    )

    initYtClient(client)
  }

  private def initYtClient(client: YtClient): YtClient = {
    try {
      client.waitProxies.join
      client
    } catch {
      case e: Throwable =>
        client.close()
        throw e
    }
  }

  private def createByopRemoteProxiesClient(connector: DefaultBusConnector,
                                            rpcOptions: RpcOptions,
                                            config: YtClientConfiguration,
                                            byopDiscoveryEndpoint: HostAndPort): YtClient = {
    val cluster = new YtCluster(
      s"${config.shortProxy}-byop",
      byopDiscoveryEndpoint.getHostText,
      byopDiscoveryEndpoint.getPort
    )

    rpcOptions.setPreferableDiscoveryMethod(DiscoveryMethod.HTTP)

    val client = new YtClient(
      connector,
      cluster,
      config.rpcCredentials,
      rpcOptions
    )

    initYtClient(client)
  }

  implicit class RichRpcOptions(options: RpcOptions) {
    def setTimeouts(timeout: Duration): RpcOptions = {
      options.setGlobalTimeout(toJavaDuration(timeout))
      options.setStreamingReadTimeout(toJavaDuration(timeout))
      options.setStreamingWriteTimeout(toJavaDuration(timeout))
    }
  }

}
