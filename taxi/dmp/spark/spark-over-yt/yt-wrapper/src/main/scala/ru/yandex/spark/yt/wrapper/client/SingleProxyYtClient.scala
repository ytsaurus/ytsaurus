package ru.yandex.spark.yt.wrapper.client

import java.util
import java.util.concurrent.CompletableFuture

import io.netty.channel.nio.NioEventLoopGroup
import ru.yandex.spark.yt.wrapper.YtJavaConverters.toJavaDuration
import ru.yandex.yt.ytclient.bus.{BusConnector, DefaultBusConnector}
import ru.yandex.yt.ytclient.proxy.internal.{FailureDetectingRpcClient, HostPort, RpcClientFactory, RpcClientFactoryImpl}
import ru.yandex.yt.ytclient.proxy.{YtClient, YtCluster}
import ru.yandex.yt.ytclient.rpc.{RpcClient, RpcCompression, RpcCredentials, RpcError, RpcOptions}

import scala.concurrent.duration._
import scala.language.postfixOps

class SingleProxyYtClient(connector: BusConnector,
                          rpcCredentials: RpcCredentials,
                          rpcOptions: RpcOptions,
                          address: HostPort) extends
  YtClient(connector, new util.ArrayList[YtCluster](), "single_proxy", rpcCredentials, rpcOptions) {

  private lazy val rpcClientFactory = new RpcClientFactoryImpl(connector, rpcCredentials, new RpcCompression)
  private lazy val client = rpcClientFactory.create(address, "single_proxy")

  override def waitProxiesImpl(): CompletableFuture[Void] = CompletableFuture.completedFuture(null)

  override def selectDestinations(): util.List[RpcClient] = {
    import scala.collection.JavaConverters._
    Seq(client).asJava
  }
}

object SingleProxyYtClient {
  def apply(address: String, rpcCredentials: RpcCredentials): SingleProxyYtClient = {
    import ru.yandex.spark.yt.wrapper.YtWrapper._
    val connector = new DefaultBusConnector(new NioEventLoopGroup(1), true)
      .setReadTimeout(toJavaDuration(300 seconds))
      .setWriteTimeout(toJavaDuration(300 seconds))

    val rpcOptions = new RpcOptions()
    rpcOptions.setTimeouts(300 seconds)

    new SingleProxyYtClient(connector, rpcCredentials, rpcOptions, HostPort.parse(address))
  }
}