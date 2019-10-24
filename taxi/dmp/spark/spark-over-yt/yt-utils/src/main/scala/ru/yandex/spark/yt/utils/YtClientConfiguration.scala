package ru.yandex.spark.yt.utils

import ru.yandex.yt.ytclient.rpc.RpcCredentials

case class YtClientConfiguration(proxy: String, user: String, token: String, timeout: Int) {
  def shortProxy: String = proxy.split("\\.").head

  def rpcCredentials: RpcCredentials = new RpcCredentials(user, token)
}

object YtClientConfiguration {
  def apply(getByName: String => Option[String]): YtClientConfiguration = {
    YtClientConfiguration(
      getByName("proxy").orElse(sys.env.get("YT_PROXY")).getOrElse(throw new IllegalArgumentException("Proxy must be specified")),
      getByName("user").orElse(sys.env.get("YT_SECURE_VAULT_YT_USER")).getOrElse(DefaultRpcCredentials.user),
      getByName("token").orElse(sys.env.get("YT_SECURE_VAULT_YT_TOKEN")).getOrElse(DefaultRpcCredentials.token),
      getByName("timeout").map(_.toInt).getOrElse(60)
    )
  }
}

