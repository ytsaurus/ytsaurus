package ru.yandex.spark.yt.test

import org.scalatest.{BeforeAndAfterAll, TestSuite}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.{ByopConfiguration, ByopRemoteConfiguration, EmptyWorkersListStrategy, YtClientConfiguration, YtRpcClient}
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.concurrent.duration._
import scala.language.postfixOps

trait LocalYtClient extends BeforeAndAfterAll {
  self: TestSuite =>

  protected def conf: YtClientConfiguration = YtClientConfiguration(
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

  protected def ytClient: YtRpcClient = YtWrapper.createRpcClient(conf)
  implicit val yt: YtClient = ytClient.yt

  override protected def afterAll(): Unit = {
    super.afterAll()
    ytClient.close()
  }
}
