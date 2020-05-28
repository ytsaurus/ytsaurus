package ru.yandex.spark.yt.wrapper.client


import java.util.{ArrayList => JArrayList}

import com.google.common.net.HostAndPort
import io.netty.channel.nio.NioEventLoopGroup
import org.apache.log4j.Logger
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.inside.yt.kosher.impl.{YtUtils => InsideYtUtils}
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.yt.ytclient.bus.DefaultBusConnector
import ru.yandex.yt.ytclient.proxy.internal.{DiscoveryMethod, HostPort}
import ru.yandex.yt.ytclient.proxy.{YtClient, YtCluster}
import ru.yandex.yt.ytclient.rpc.RpcOptions

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

trait YtClientUtils {
  private val log = Logger.getLogger(getClass)

  def createRpcClient(config: YtClientConfiguration): YtRpcClient = {
    log.info(s"Create RPC YT Client, configuration ${config.copy(token = "*****")}")
    val connector = new DefaultBusConnector(new NioEventLoopGroup(1), true)
      .setReadTimeout(toJavaDuration(config.timeout))
      .setWriteTimeout(toJavaDuration(config.timeout))

    try {
      val rpcOptions = new RpcOptions()
      rpcOptions.setTimeouts(config.timeout)

      val client = createYtClient(config, connector, rpcOptions)

      try {
        client.waitProxies.join
        log.info("YtClient created")
        YtRpcClient(client, connector)
      } catch {
        case e: Throwable =>
          client.close()
          throw e
      }
    } catch {
      case e: Throwable =>
        connector.close()
        throw e
    }
  }

  private def createYtClient(config: YtClientConfiguration,
                             connector: DefaultBusConnector,
                             rpcOptions: RpcOptions): YtClient = {
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
    if (config.byop.enabled && sys.env.get("SPARK_YT_BYOP_ENABLED").exists(_.toBoolean)) {
      for {
        host <- sys.env.get("SPARK_YT_BYOP_HOST")
        port <- sys.env.get("SPARK_YT_BYOP_PORT").map(_.toInt)
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
                                         byopEndpoint: HostAndPort): YtClient = {
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

    new YtClient(
      connector,
      cluster,
      config.rpcCredentials,
      rpcOptions
    )
  }

  private def createByopRemoteProxiesClient(connector: DefaultBusConnector,
                                            rpcOptions: RpcOptions,
                                            config: YtClientConfiguration,
                                            byopDiscoveryEndpoint: HostAndPort): YtClient = {
    val cluster = new YtCluster(
      s"${config.shortProxy}-byop",
      byopDiscoveryEndpoint.getHost,
      byopDiscoveryEndpoint.getPort
    )

    rpcOptions.setPreferableDiscoveryMethod(DiscoveryMethod.HTTP)

    new YtClient(
      connector,
      cluster,
      config.rpcCredentials,
      rpcOptions
    )
  }

  def createHttpClient(config: YtClientConfiguration): Yt = {
    InsideYtUtils.http(s"${config.fullProxy}:${config.port}", config.token)
  }

  implicit class RichRpcOptions(options: RpcOptions) {
    def setTimeouts(timeout: Duration): RpcOptions = {
      options.setGlobalTimeout(toJavaDuration(timeout))
      options.setStreamingReadTimeout(toJavaDuration(timeout))
      options.setStreamingWriteTimeout(toJavaDuration(timeout))
    }
  }

}
