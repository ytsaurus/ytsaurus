package tech.ytsaurus.spyt.e2e

import org.scalatest.TestSuite
import tech.ytsaurus.spyt.e2e.E2EYtClient.ytProxy
import tech.ytsaurus.spyt.wrapper.client._
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

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
  lazy val ytProxy: String = System.getProperty("proxies")
}
