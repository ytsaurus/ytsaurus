package ru.yandex.spark.yt.wrapper.client

import io.netty.channel.nio.NioEventLoopGroup
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.spark.yt.test.{LocalYtClient, TestUtils, TmpDir}
import ru.yandex.spark.yt.wrapper.YtJavaConverters.toJavaDuration
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.bus.DefaultBusConnector
import ru.yandex.yt.ytclient.proxy.internal.HostPort
import ru.yandex.yt.ytclient.rpc.{RpcCredentials, RpcOptions}
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

import scala.concurrent.duration._
import scala.language.postfixOps

class SingleProxyYtClientTest extends FlatSpec with Matchers with LocalYtClient with TmpDir with TestUtils {
  private val singleProxyYtClient = createClient("localhost:8002", new RpcCredentials("root", ""))

  "SingleProxyYtClient" should "list nodes" in {
    YtWrapper.createDir(tmpPath)
    YtWrapper.createDir(s"$tmpPath/1")
    YtWrapper.createDir(s"$tmpPath/2")
    YtWrapper.createDir(s"$tmpPath/3")

    val res = YtWrapper.listDir(tmpPath)(singleProxyYtClient)

    res should contain theSameElementsAs Seq("1", "2", "3")
  }

  it should "write and read table" in {
    val schema = new TableSchema.Builder()
      .addValue("a", ColumnValueType.STRING)
      .addValue("b", ColumnValueType.INT64)
      .setUniqueKeys(false)
      .build()
    val data = Seq(
      """{"a"="AAA";"b"=123}""",
      """{"a"="BBB";"b"=456}"""
    )

    writeTableFromYson(data, tmpPath, schema)(singleProxyYtClient)
    val res = readTableAsYson(tmpPath, schema)(singleProxyYtClient).map(YTreeTextSerializer.serialize)

    res should contain theSameElementsAs data
  }

  def createClient(address: String, rpcCredentials: RpcCredentials): SingleProxyYtClient = {
    import ru.yandex.spark.yt.wrapper.YtWrapper._
    val connector = new DefaultBusConnector(new NioEventLoopGroup(1), true)
      .setReadTimeout(toJavaDuration(300 seconds))
      .setWriteTimeout(toJavaDuration(300 seconds))

    val rpcOptions = new RpcOptions()
    rpcOptions.setTimeouts(300 seconds)

    new SingleProxyYtClient(connector, rpcCredentials, rpcOptions, HostPort.parse(address))
  }
}
