package ru.yandex.spark.yt.test

import org.scalatest.TestSuite
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client._
import ru.yandex.yt.ytclient.proxy.CompoundClient

import scala.concurrent.duration._
import scala.language.postfixOps

trait LocalYtClientCreator {
  self: TestSuite =>
  protected implicit val yt: CompoundClient
}

trait LocalYtClient extends LocalYtClientCreator {
  self: TestSuite =>
  protected lazy val ytClient: YtRpcClient = LocalYtClient.ytClient
  override protected implicit lazy val yt: CompoundClient = ytClient.yt
}

object LocalYtClient {
  val conf: YtClientConfiguration = YtClientConfiguration(
    proxy = "localhost:8000",
    user = "root",
    token = "",
    timeout = 5 minutes,
    proxyRole = None,
    byop = ByopConfiguration(
      enabled = false,
      ByopRemoteConfiguration(enabled = false, EmptyWorkersListStrategy.Default)
    ),
    masterWrapperUrl = None,
    extendedFileTimeout = true
  )

  lazy val ytClient: YtRpcClient = YtWrapper.createRpcClient("test", conf)
}
