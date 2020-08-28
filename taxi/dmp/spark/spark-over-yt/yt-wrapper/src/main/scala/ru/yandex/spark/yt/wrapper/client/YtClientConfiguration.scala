package ru.yandex.spark.yt.wrapper.client

import ru.yandex.spark.yt.wrapper.Utils
import ru.yandex.yt.ytclient.rpc.RpcCredentials

import scala.concurrent.duration._
import scala.language.postfixOps

case class YtClientConfiguration(proxy: String,
                                 user: String,
                                 token: String,
                                 timeout: Duration,
                                 proxyRole: Option[String],
                                 byop: ByopConfiguration,
                                 masterWrapperUrl: Option[String]) {
  def shortProxy: String = proxy.split("\\.").head.split(":").head

  def fullProxy: String = if (proxy.contains(".") || proxy.contains(":")) {
    proxy.split(":").head
  } else shortProxy + ".yt.yandex.net"

  def port: Int = proxy.split(":").lift(1).map(_.toInt).getOrElse(80)

  def rpcCredentials: RpcCredentials = new RpcCredentials(user, token)
}

object YtClientConfiguration {
  def parseEmptyWorkersListStrategy(getByName: String => Option[String]): EmptyWorkersListStrategy = {
    val strategyFromConf = for {
      name <- getByName("byop.remote.emptyWorkersList.strategy")
      timeout <- getByName("byop.remote.emptyWorkersList.timeout").map(Utils.parseDuration)
    } yield EmptyWorkersListStrategy.fromString(name, timeout)

    strategyFromConf.getOrElse(EmptyWorkersListStrategy.Default)
  }

  def apply(getByName: String => Option[String]): YtClientConfiguration = {
    val byopEnabled = getByName("byop.enabled").forall(_.toBoolean)

    YtClientConfiguration(
      getByName("proxy").orElse(sys.env.get("YT_PROXY")).getOrElse(
        throw new IllegalArgumentException("Proxy must be specified")
      ),
      getByName("user").orElse(sys.env.get("YT_SECURE_VAULT_YT_USER")).getOrElse(DefaultRpcCredentials.user),
      getByName("token").orElse(sys.env.get("YT_SECURE_VAULT_YT_TOKEN")).getOrElse(DefaultRpcCredentials.token),
      getByName("timeout").map(Utils.parseDuration).getOrElse(60 seconds),
      getByName("proxyRole"),
      ByopConfiguration(
        enabled = byopEnabled,
        ByopRemoteConfiguration(
          enabled = getByName("byop.remote.enabled").map(_.toBoolean).getOrElse(byopEnabled),
          emptyWorkersListStrategy = parseEmptyWorkersListStrategy(getByName)
        )
      ),
      getByName("masterWrapper.url")
    )
  }

  def default(proxy: String): YtClientConfiguration = YtClientConfiguration(
    proxy = proxy,
    user = DefaultRpcCredentials.user,
    token = DefaultRpcCredentials.token,
    timeout = 5 minutes,
    proxyRole = None,
    byop = ByopConfiguration.DISABLED,
    masterWrapperUrl = None
  )
}

