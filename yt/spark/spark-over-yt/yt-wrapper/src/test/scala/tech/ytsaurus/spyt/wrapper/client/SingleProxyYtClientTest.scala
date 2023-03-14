package tech.ytsaurus.spyt.wrapper.client

import io.netty.channel.nio.NioEventLoopGroup
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.client.bus.DefaultBusConnector
import tech.ytsaurus.client.rpc.{RpcOptions, YTsaurusClientAuth}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.HostAndPort
import tech.ytsaurus.spyt.test.{LocalYt, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtJavaConverters.toJavaDuration
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.ysontree.YTreeTextSerializer

import scala.concurrent.duration._
import scala.language.postfixOps

class SingleProxyYtClientTest extends FlatSpec with Matchers with LocalYt with TmpDir with TestUtils {

  override protected implicit val ytRpcClient: YtRpcClient = {
    createClient(
      s"${LocalYt.host}:${LocalYt.rpcProxyPort}",
      YTsaurusClientAuth.builder().setUser("root").setToken("").build()
    )
  }

  "SingleProxyYtClient" should "create and list nodes" in {
    YtWrapper.createDir(tmpPath)
    YtWrapper.createDir(s"$tmpPath/1")
    YtWrapper.createDir(s"$tmpPath/2")
    YtWrapper.createDir(s"$tmpPath/3")

    val res = YtWrapper.listDir(tmpPath)

    res should contain theSameElementsAs Seq("1", "2", "3")
  }

  it should "write and read table" in {
    val schema = TableSchema.builder()
      .addValue("a", ColumnValueType.STRING)
      .addValue("b", ColumnValueType.INT64)
      .setUniqueKeys(false)
      .build()
    val data = Seq(
      """{"a"="AAA";"b"=123;}""",
      """{"a"="BBB";"b"=456;}"""
    )

    writeTableFromYson(data, tmpPath, schema)
    val res = readTableAsYson(tmpPath).map(YTreeTextSerializer.serialize)

    res should contain theSameElementsAs data
  }

  def createClient(address: String, clientAuth: YTsaurusClientAuth): YtRpcClient = {
    import tech.ytsaurus.spyt.wrapper.YtWrapper._
    val connector = new DefaultBusConnector(new NioEventLoopGroup(1), true)
      .setReadTimeout(toJavaDuration(300 seconds))
      .setWriteTimeout(toJavaDuration(300 seconds))

    val rpcOptions = new RpcOptions()
    rpcOptions.setTimeouts(300 seconds)

    YtRpcClient(
      "single",
      new SingleProxyYtClient(connector, clientAuth, rpcOptions, HostAndPort.fromString(address)),
      connector
    )
  }
}
