package ru.yandex.spark.yt.test

import org.scalatest.{BeforeAndAfterAll, TestSuite}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.{ByopConfiguration, ByopRemoteConfiguration, EmptyWorkersListStrategy, YtClientConfiguration, YtRpcClient}
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.concurrent.duration._
import scala.language.postfixOps

trait LocalYtClient {
  self: TestSuite =>
  protected lazy val ytClient: YtRpcClient = LocalYtClient.ytClient
  protected implicit lazy val yt: YtClient = ytClient.yt
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
    masterWrapperUrl = None
  )

  lazy val ytClient: YtRpcClient = YtWrapper.createRpcClient(conf)
}
