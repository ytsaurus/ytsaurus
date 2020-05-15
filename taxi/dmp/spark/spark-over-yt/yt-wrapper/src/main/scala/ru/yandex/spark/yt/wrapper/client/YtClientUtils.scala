package ru.yandex.spark.yt.wrapper.client


import java.util.{Optional, ArrayList => JArrayList}

import io.netty.channel.nio.NioEventLoopGroup
import org.apache.log4j.Logger
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.inside.yt.kosher.impl.{YtUtils => InsideYtUtils}
import ru.yandex.yt.ytclient.bus.DefaultBusConnector
import ru.yandex.yt.ytclient.proxy.{YtClient, YtCluster}
import ru.yandex.yt.ytclient.rpc.RpcOptions
import ru.yandex.spark.yt.wrapper.YtJavaConverters._

import scala.concurrent.duration.Duration

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

      val client = byopConfig(config) match {
        case Some(byop) =>
          log.info(s"Create BYOP client with config $byop")
          createByopProxyClient(connector, rpcOptions, config, byop)
        case None =>
          log.info(s"Create remote proxies client")
          createRemoteProxiesClient(connector, rpcOptions, config)
      }

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

  private def byopConfig(config: YtClientConfiguration): Option[YtByopConfiguration] = {
    if (config.byopEnabled && sys.env.get("SPARK_YT_BYOP_ENABLED").exists(_.toBoolean)) {
      for {
        host <- sys.env.get("SPARK_YT_BYOP_HOST")
        port <- sys.env.get("SPARK_YT_BYOP_PORT").map(_.toInt)
      } yield YtByopConfiguration(host, port)
    } else None
  }

  private def createByopProxyClient(connector: DefaultBusConnector,
                                    rpcOptions: RpcOptions,
                                    config: YtClientConfiguration,
                                    byopConfig: YtByopConfiguration): YtClient = {
    new SingleProxyYtClient(
      connector,
      config.rpcCredentials,
      rpcOptions,
      byopConfig.address
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
      config.proxyRole.map(Optional.of[String]).getOrElse(Optional.empty[String]))

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
