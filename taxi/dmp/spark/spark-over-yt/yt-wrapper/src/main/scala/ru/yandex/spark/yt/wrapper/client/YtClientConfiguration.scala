package ru.yandex.spark.yt.wrapper.client

import ru.yandex.yt.ytclient.rpc.RpcCredentials

import scala.concurrent.duration._
import scala.language.postfixOps

case class YtClientConfiguration(proxy: String,
                                 user: String,
                                 token: String,
                                 timeout: Duration,
                                 proxyRole: Option[String]) {
  def shortProxy: String = proxy.split("\\.").head.split(":").head

  def fullProxy: String = if (proxy.contains(".") || proxy.contains(":")) {
    proxy.split(":").head
  } else shortProxy + ".yt.yandex.net"

  def port: Int = proxy.split(":").lift(1).map(_.toInt).getOrElse(80)

  def rpcCredentials: RpcCredentials = new RpcCredentials(user, token)
}

object YtClientConfiguration {
  def apply(getByName: String => Option[String]): YtClientConfiguration = {
    YtClientConfiguration(
      getByName("proxy").orElse(sys.env.get("YT_PROXY")).getOrElse(throw new IllegalArgumentException("Proxy must be specified")),
      getByName("user").orElse(sys.env.get("YT_SECURE_VAULT_YT_USER")).getOrElse(DefaultRpcCredentials.user),
      getByName("token").orElse(sys.env.get("YT_SECURE_VAULT_YT_TOKEN")).getOrElse(DefaultRpcCredentials.token),
      getByName("timeout").map(_.toInt.seconds).getOrElse(60 seconds),
      getByName("proxyRole")
    )
  }
}

