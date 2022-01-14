package ru.yandex.spark.yt.e2e

import org.scalatest.TestSuite
import ru.yandex.spark.yt.test.LocalYtClient
import ru.yandex.spark.yt.wrapper.client._
import ru.yandex.yt.ytclient.proxy.CompoundClient

import scala.concurrent.duration._
import scala.language.postfixOps

trait HumeYtClient {
  self: TestSuite =>

  private val conf: YtClientConfiguration = YtClientConfiguration(
    proxy = "hume",
    user = DefaultRpcCredentials.user,
    token = DefaultRpcCredentials.token,
    timeout = 5 minutes,
    proxyRole = None,
    byop = ByopConfiguration(
      enabled = false,
      ByopRemoteConfiguration(
        enabled = false,
        EmptyWorkersListStrategy.Default
      )
    ),
    masterWrapperUrl = None,
    extendedFileTimeout = true
  )

  protected val ytRpcClient: YtRpcClient = {
    YtClientProvider.ytRpcClient(conf, "test")
  }

  protected implicit val yt: CompoundClient = ytRpcClient.yt
}
