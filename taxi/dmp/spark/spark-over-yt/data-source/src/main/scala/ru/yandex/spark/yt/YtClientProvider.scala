package ru.yandex.spark.yt


import java.time.Duration

import io.netty.channel.nio.NioEventLoopGroup
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import ru.yandex.bolts.collection.impl.ReadOnlyArrayList
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.inside.yt.kosher.impl.YtUtils
import ru.yandex.yt.ytclient.bus.DefaultBusConnector
import ru.yandex.yt.ytclient.proxy.{YtClient, YtCluster}
import ru.yandex.yt.ytclient.rpc.{RpcCredentials, RpcOptions}


object YtClientProvider {
  private val log = Logger.getLogger(getClass)
  private val _client: ThreadLocal[Option[YtClient]] = new ThreadLocal[Option[YtClient]]
  private val _proxy: ThreadLocal[Option[String]] = new ThreadLocal[Option[String]]
  private val _token: ThreadLocal[Option[String]] = new ThreadLocal[Option[String]]


  def ytClient(proxy: String,
               rpcCredentials: RpcCredentials,
               threads: Int,
               timeout: Duration = Duration.ofMinutes(1)): YtClient = Option(_client.get()).flatten.getOrElse {
    _proxy.set(Some(proxy))
    _token.set(Some(rpcCredentials.getToken))
    log.debugLazy("Create yt client")
    val connector = new DefaultBusConnector(new NioEventLoopGroup(threads), true)
    val rpcOptions = new RpcOptions()
    rpcOptions.setGlobalTimeout(timeout)

    val cluster = new YtCluster(proxy.split("\\.").head)
    val client = new YtClient(
      connector,
      ReadOnlyArrayList.cons(cluster),
      cluster.getName,
      null,
      rpcCredentials,
      //new RpcCompression(Compression.Lz4),
      rpcOptions
    )

    client.waitProxies.join
    log.debugLazy("YtClient created")
    _client.set(Some(client))
    _client.get.get
  }

  def ytClient: YtClient = Option(_client.get()).flatten.getOrElse(throw new IllegalStateException("YtClient is not initialized"))

  def ytClient(conf: YtClientConfiguration): YtClient = {
    ytClient(conf.proxy, new RpcCredentials(conf.user, conf.token), 1, Duration.ofMinutes(conf.timeoutMinutes))
  }

  def httpClient: Yt = YtUtils.http(_proxy.get().get, _token.get().get)
}

case class YtClientConfiguration(proxy: String, user: String, token: String, timeoutMinutes: Int)

object YtClientConfiguration {

  import ru.yandex.spark.yt.format.SparkYtOptions._

  def apply(spark: SparkSession): YtClientConfiguration = {
    apply { (name, default) => spark.sqlContext.getYtConf(name, default()) }
  }

  def apply(conf: Configuration): YtClientConfiguration = {
    apply { (name, default) => conf.getYtConf(name, default()) }
  }

  private def apply[T](getByName: (String, () => String) => String): YtClientConfiguration = {
    new YtClientConfiguration(
      getByName("proxy", () => throw new IllegalArgumentException("Proxy must be specified")),
      getByName("user", () => DefaultRpcCredentials.user),
      getByName("token", () => DefaultRpcCredentials.token),
      getByName("timeout", () => "60").toInt
    )
  }
}
