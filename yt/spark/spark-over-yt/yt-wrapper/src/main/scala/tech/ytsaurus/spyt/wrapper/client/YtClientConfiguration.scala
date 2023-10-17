package tech.ytsaurus.spyt.wrapper.client

import tech.ytsaurus.client.YTsaurusCluster

import java.time.{Duration => JDuration}
import tech.ytsaurus.spyt.wrapper.Utils
import tech.ytsaurus.spyt.wrapper.YtJavaConverters.toScalaDuration
import tech.ytsaurus.client.rpc.YTsaurusClientAuth

import java.net.URL
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

@SerialVersionUID(-7486028686763336923L)
case class YtClientConfiguration(proxy: String,
                                 user: String,
                                 token: String,
                                 timeout: Duration,
                                 proxyRole: Option[String],
                                 byop: ByopConfiguration,
                                 masterWrapperUrl: Option[String],
                                 extendedFileTimeout: Boolean) {

  private def proxyUrl: Try[URL] = Try(new URL(proxy)).orElse {
    val normalizedProxy = if (proxy.contains(".") || proxy.contains(":")) {
      proxy
    } else {
      s"$proxy.yt.yandex.net"
    }
    Try(new URL(s"http://$normalizedProxy"))
  }

  def fullProxy: String = proxyUrl.map(_.getHost).get

  def port: Int = proxyUrl.map { url =>
    if (url.getPort != -1) url.getPort else if (url.getDefaultPort != -1) url.getDefaultPort else 80
  }.get

  def isHttps: Boolean = proxyUrl.toOption.exists(_.getProtocol == "https")

  def clientAuth: YTsaurusClientAuth = YTsaurusClientAuth.builder().setUser(user).setToken(token).build()
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
    val byopEnabled = getByName("byop.enabled").orElse(sys.env.get("SPARK_YT_BYOP_ENABLED")).exists(_.toBoolean)

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

