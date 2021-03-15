package ru.yandex.spark.yt.wrapper.client

import java.net.InetSocketAddress
import java.util.concurrent.CompletableFuture

import com.google.protobuf.MessageLite
import io.netty.channel.nio.NioEventLoopGroup
import ru.yandex.spark.yt.wrapper.YtJavaConverters.toJavaDuration
import ru.yandex.yt.ytclient.bus.{BusConnector, DefaultBusConnector}
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.proxy.internal.HostPort
import ru.yandex.yt.ytclient.rpc._

import scala.concurrent.duration._
import scala.language.postfixOps

class SingleProxyYtClient(connector: BusConnector,
                          rpcCredentials: RpcCredentials,
                          rpcOptions: RpcOptions,
                          address: HostPort) extends CompoundClient(connector.executorService(), rpcOptions) {

  private val client = SingleProxyYtClient.createClient(address, connector, rpcCredentials)
  private val rpcClientPool = new RpcClientPool {
    override def peekClient(completableFuture: CompletableFuture[_]): CompletableFuture[RpcClient] = {
      CompletableFuture.completedFuture(client)
    }
  }

  override def invoke[RequestType <: MessageLite.Builder, ResponseType]
  (builder: RpcClientRequestBuilder[RequestType, ResponseType]): CompletableFuture[ResponseType] = {
    builder.invokeVia(connector.executorService(), rpcClientPool)
  }

  override def startStream[RequestType <: MessageLite.Builder, ResponseType]
  (builder: RpcClientRequestBuilder[RequestType, ResponseType],
   consumer: RpcStreamConsumer): CompletableFuture[RpcClientStreamControl] = {
    builder.startStream(connector.executorService(), rpcClientPool, consumer)
  }


  override def close(): Unit = {
    client.close()
  }

  override def toString: String = {
    s"SingleProxyYtClient: ${super.toString}"
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

  def createClient(address: HostPort,
                   connector: BusConnector,
                   rpcCredentials: RpcCredentials): RpcClient = {
    new DefaultRpcBusClient(connector, new InetSocketAddress(address.getHost, address.getPort), "single_proxy")
      .withTokenAuthentication(rpcCredentials)
  }
}