package tech.ytsaurus.spyt.launcher

import org.scalatest.TestSuite
import tech.ytsaurus.spyt.test.LocalYtClient
import tech.ytsaurus.spyt.wrapper.client.{ByopConfiguration, ByopRemoteConfiguration, DefaultRpcCredentials, EmptyWorkersListStrategy, YtClientConfiguration}

import scala.concurrent.duration._
import scala.language.postfixOps

trait HumeYtClient extends LocalYtClient {
  self: TestSuite =>

  override protected def conf: YtClientConfiguration = YtClientConfiguration(
    proxy = "hume",
    user = DefaultRpcCredentials.user,
    token = DefaultRpcCredentials.token,
    timeout = 5 minutes,
    proxyRole = None,
    byop = ByopConfiguration.DISABLED,
    masterWrapperUrl = None
  )
}
