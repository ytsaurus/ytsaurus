package ru.yandex.spark.yt.wrapper.client

import java.time.{Duration => JDuration}

import ru.yandex.spark.yt.wrapper.Utils
import ru.yandex.spark.yt.wrapper.YtJavaConverters.toScalaDuration
import ru.yandex.yt.ytclient.rpc.RpcCredentials

import scala.concurrent.duration._
import scala.language.postfixOps

@SerialVersionUID(-7486028686763336923L)
case class YtClientConfiguration(proxy: String,
                                 user: String,
                                 token: String,
                                 timeout: Duration,
                                 proxyRole: Option[String],
                                 byop: ByopConfiguration,
                                 masterWrapperUrl: Option[String],
                                 extendedFileTimeout: Boolean) {
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

  def apply(getByName: String => Option[String], proxy: Option[String] = None): YtClientConfiguration = {
    val byopEnabled = getByName("byop.enabled").forall(_.toBoolean)

    YtClientConfiguration(
      proxy.getOrElse(getByName("proxy").orElse(sys.env.get("YT_PROXY")).getOrElse(
        throw new IllegalArgumentException("Proxy must be specified")
      )),
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
      getByName("masterWrapper.url"),
      getByName("extendedFileTimeout").forall(_.toBoolean)
    )
  }

  def default(proxy: String): YtClientConfiguration = default(
    proxy = proxy,
    user = DefaultRpcCredentials.user,
    token = DefaultRpcCredentials.token
  )

  def default(proxy: String, user: String, token: String): YtClientConfiguration = YtClientConfiguration(
    proxy = proxy,
    user = user,
    token = token,
    timeout = 5 minutes,
    proxyRole = None,
    byop = ByopConfiguration.DISABLED,
    masterWrapperUrl = None,
    extendedFileTimeout = true
  )

  def create(proxy: String,
             user: String,
             token: String,
             timeout: JDuration,
             proxyRole: String,
             byop: ByopConfiguration,
             masterWrapperUrl: String,
             extendedFileTimeout: Boolean) = new YtClientConfiguration(
    proxy, user, token, toScalaDuration(timeout),
    Option(proxyRole), byop, Option(masterWrapperUrl), extendedFileTimeout
  )
}

