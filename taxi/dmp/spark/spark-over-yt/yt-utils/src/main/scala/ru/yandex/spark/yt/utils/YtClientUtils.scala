package ru.yandex.spark.yt.utils

import java.time.{Duration => JavaDuration}

import io.netty.channel.nio.NioEventLoopGroup
import org.apache.log4j.Logger
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.inside.yt.kosher.impl.{YtUtils => InsideYtUtils}
import ru.yandex.yt.ytclient.bus.DefaultBusConnector
import ru.yandex.yt.ytclient.proxy.YtClient
import ru.yandex.yt.ytclient.rpc.RpcOptions

import scala.concurrent.duration.Duration

object YtClientUtils {
  private val log = Logger.getLogger(getClass)

  def createRpcClient(config: YtClientConfiguration): YtRpcClient = {
    log.info("Create yt client")
    val connector = new DefaultBusConnector(new NioEventLoopGroup(1), true)
      .setReadTimeout(javaDuration(config.timeout))
      .setWriteTimeout(javaDuration(config.timeout))
    try {
      val rpcOptions = new RpcOptions()
      rpcOptions.setTimeouts(config.timeout)

      val client = new YtClient(
        connector,
        config.shortProxy,
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
    InsideYtUtils.http(config.fullProxy, config.token)
  }

  def javaDuration(timeout: Duration): JavaDuration = {
    JavaDuration.ofMillis(timeout.toMillis)
  }

  implicit class RichRpcOptions(options: RpcOptions) {
    def setTimeouts(timeout: Duration): RpcOptions = {
      options.setGlobalTimeout(javaDuration(timeout))
      options.setStreamingReadTimeout(javaDuration(timeout))
      options.setStreamingWriteTimeout(javaDuration(timeout))
    }
  }
}

case class YtRpcClient(yt: YtClient, connector: DefaultBusConnector) extends AutoCloseable {
  def close(): Unit = {
    yt.close()
    connector.close()
  }
}
