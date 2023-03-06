package tech.ytsaurus.spyt.wrapper.client

import com.google.protobuf.MessageLite
import io.netty.channel.nio.NioEventLoopGroup
import tech.ytsaurus.spyt.HostAndPort
import tech.ytsaurus.spyt.wrapper.YtJavaConverters.toJavaDuration
import tech.ytsaurus.client.{CompoundClientImpl, DefaultSerializationResolver}
import tech.ytsaurus.client.bus.{BusConnector, DefaultBusConnector}
import tech.ytsaurus.client.rpc._

import java.net.InetSocketAddress
import java.util.concurrent.{CompletableFuture, ForkJoinPool}
import scala.concurrent.duration._
import scala.language.postfixOps

class SingleProxyYtClient(connector: BusConnector,
                          rpcCredentials: RpcCredentials,
                          rpcOptions: RpcOptions,
                          address: HostAndPort)
  extends CompoundClientImpl(connector.executorService(), rpcOptions, ForkJoinPool.commonPool, DefaultSerializationResolver.getInstance()) {

  private val client = SingleProxyYtClient.createClient(address, connector, rpcCredentials)
  private val rpcClientPool = new RpcClientPool {
    override def peekClient(completableFuture: CompletableFuture[_]): CompletableFuture[RpcClient] = {
      CompletableFuture.completedFuture(client)
    }
  }

  override def invoke[RequestType <: MessageLite.Builder, ResponseType <: MessageLite]
  (builder: RpcClientRequestBuilder[RequestType, ResponseType]): CompletableFuture[RpcClientResponse[ResponseType]] = {
    builder.invokeVia(connector.executorService(), rpcClientPool)
  }

  override def startStream[RequestType <: MessageLite.Builder, ResponseType <: MessageLite]
  (builder: RpcClientRequestBuilder[RequestType, ResponseType],
   consumer: RpcStreamConsumer): CompletableFuture[RpcClientStreamControl] = {
    val control = client.startStream(client, builder.getRpcRequest, consumer, builder.getOptions)
    CompletableFuture.completedFuture(control)
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
    import tech.ytsaurus.spyt.wrapper.YtWrapper._
    val connector = new DefaultBusConnector(new NioEventLoopGroup(1), true)
      .setReadTimeout(toJavaDuration(300 seconds))
      .setWriteTimeout(toJavaDuration(300 seconds))

    val rpcOptions = new RpcOptions()
    rpcOptions.setTimeouts(300 seconds)

    new SingleProxyYtClient(connector, rpcCredentials, rpcOptions, HostAndPort.fromString(address))
  }

  def createClient(address: HostAndPort,
                   connector: BusConnector,
                   rpcCredentials: RpcCredentials): RpcClient = {
    new DefaultRpcBusClient(connector, new InetSocketAddress(address.host, address.port), "single_proxy")
      .withTokenAuthentication(rpcCredentials)
  }
}