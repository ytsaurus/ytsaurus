package ru.yandex.spark.yt


import java.time.Duration

import io.netty.channel.nio.NioEventLoopGroup
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import ru.yandex.yt.ytclient.bus.DefaultBusConnector
import ru.yandex.yt.ytclient.proxy.YtClient
import ru.yandex.yt.ytclient.rpc.{RpcCredentials, RpcOptions}


object YtClientProvider {
  private val log = Logger.getLogger(getClass)
  private val _client: ThreadLocal[Option[YtClient]] = new ThreadLocal[Option[YtClient]]

  def ytClient(proxy: String, rpcCredentials: RpcCredentials, threads: Int, timeout: Duration = Duration.ofMinutes(1)): YtClient = Option(_client.get()).flatten.getOrElse{
    log.info("Create yt client")
    val connector = new DefaultBusConnector(new NioEventLoopGroup(threads), true)
    val rpcOptions = new RpcOptions()
    rpcOptions.setGlobalTimeout(timeout)
    val client = new YtClient(connector, proxy, rpcCredentials, rpcOptions)
    client.waitProxies.join
    log.info("YtClient created")
    _client.set(Some(client))
    _client.get.get
  }

  def ytClient(parameters: DefaultSourceParameters, timeout: Duration): YtClient = {
    ytClient(parameters.proxy, parameters.rpcCredentials, 1, timeout)
  }

  def ytClient(conf: Configuration): YtClient = {
    ytClient(conf.get("yt.proxy"), new RpcCredentials(conf.get("yt.user"), conf.get("yt.token")), 1,
      Duration.ofMinutes(conf.get("spark.yt.timeout.minutes").toLong))
  }

  def ytClient: YtClient = Option(_client.get()).flatten.getOrElse(throw new IllegalStateException("YtClient is not initialized"))

  def close(): Unit = {
   // _client.get().foreach(_.close())
    _client.set(None)
  }
}
