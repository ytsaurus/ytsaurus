package tech.ytsaurus.spyt

import org.scalatest.TestSuite
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client._
import tech.ytsaurus.spyt.test.LocalYtClient
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

import scala.concurrent.duration._
import scala.language.postfixOps

trait HumeYtClient extends LocalYtClient {
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

  override protected val ytRpcClient: YtRpcClient = {
    YtClientProvider.ytRpcClient(conf, "test")
  }
}
