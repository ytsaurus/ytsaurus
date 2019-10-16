package ru.yandex.spark.yt.utils

import java.time.Duration

import io.netty.channel.nio.NioEventLoopGroup
import org.apache.log4j.Logger
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.inside.yt.kosher.impl.{YtUtils => InsideYtUtils}
import ru.yandex.yt.ytclient.bus.DefaultBusConnector
import ru.yandex.yt.ytclient.proxy.YtClient
import ru.yandex.yt.ytclient.rpc.RpcOptions

object YtUtils {
  private val log = Logger.getLogger(getClass)

  def createRpcClient(config: YtClientConfiguration): YtRpcClient = {
    log.debug("Create yt client")
    val connector = new DefaultBusConnector(new NioEventLoopGroup(1), true)
    try {
      val rpcOptions = new RpcOptions()
      rpcOptions.setGlobalTimeout(Duration.ofSeconds(config.timeout))

      val client = new YtClient(
        connector,
        config.shortProxy,
        config.rpcCredentials,
        rpcOptions
      )

      try {
        client.waitProxies.join
        log.debug("YtClient created")
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
    InsideYtUtils.http(config.proxy, config.token)
  }
}

case class YtRpcClient(yt: YtClient, connector: DefaultBusConnector) {
  def close(): Unit = {
    yt.close()
    connector.close()
  }
}
