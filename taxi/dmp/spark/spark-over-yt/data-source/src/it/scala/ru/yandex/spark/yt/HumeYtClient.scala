package ru.yandex.spark.yt

import org.scalatest.TestSuite
import ru.yandex.spark.yt.test.LocalYtClient
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client._

import scala.concurrent.duration._
import scala.language.postfixOps

trait HumeYtClient extends LocalYtClient {
  self: TestSuite =>

  override protected lazy val ytClient: YtRpcClient = HumeYtClient.ytClient
}

object HumeYtClient {
  val conf: YtClientConfiguration = YtClientConfiguration(
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

  lazy val ytClient = YtWrapper.createRpcClient("test", conf)
}
