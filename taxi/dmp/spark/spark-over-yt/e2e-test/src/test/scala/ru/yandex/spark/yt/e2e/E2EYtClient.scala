package ru.yandex.spark.yt.e2e

import org.scalatest.TestSuite
import ru.yandex.spark.yt.e2e.E2EYtClient.ytProxy
import ru.yandex.spark.yt.wrapper.client._
import ru.yandex.yt.ytclient.proxy.CompoundClient

import java.util.NoSuchElementException
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.concurrent.duration._
import scala.language.postfixOps

trait E2EYtClient {
  self: TestSuite =>

  private val conf: YtClientConfiguration = YtClientConfiguration(
    proxy = ytProxy,
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

object E2EYtClient {
  lazy val ytProxy: String = {
    val envProxy = Option(System.getenv("YT_PROXY"))
    val propProxy = Option(System.getProperty("proxies"))
    val proxyList = propProxy.orElse(envProxy) match {
      case Some(value) => value.split(",").toSeq
      case None => Nil
    }
    proxyList match {
      case Nil =>
        throw new NoSuchElementException("No testing proxy provided")
      case proxy +: Nil =>
        proxy
      case _ =>
        throw new IllegalArgumentException(s"Testing on few proxies(${proxyList.mkString(",")}) is not supported")
    }
  }
}
