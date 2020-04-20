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

      val cluster = new YtCluster(
        config.shortProxy,
        config.fullProxy,
        config.port,
        new JArrayList(),
        config.proxyRole.map(Optional.of[String]).getOrElse(Optional.empty[String]))

      val client = new YtClient(
        connector,
        cluster,
        config.rpcCredentials,
        rpcOptions
      )

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
