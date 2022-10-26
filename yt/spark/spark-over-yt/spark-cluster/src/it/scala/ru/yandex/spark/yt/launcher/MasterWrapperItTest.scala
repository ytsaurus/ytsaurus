package ru.yandex.spark.yt.launcher

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import io.circe.parser._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.common.YtFormat
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.spark.discovery.CypressDiscoveryService
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.{ByopConfiguration, ByopRemoteConfiguration, EmptyWorkersListStrategy}
import sttp.client._

import scala.concurrent.duration._
import scala.language.postfixOps

class MasterWrapperItTest extends FlatSpec with Matchers with TestUtils with BeforeAndAfterAll with HumeYtClient {
  override def version: String = "0.2.2-SNAPSHOT"

  implicit private val backend = HttpURLConnectionBackend()
  private val discovery = new CypressDiscoveryService(s"${discoveryPath("wrapper")}/discovery")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    //startCluster("wrapper")
  }

  "MasterWrapper" should "be registered" in {
    discovery.masterWrapperEndpoint().nonEmpty shouldEqual true
  }

  it should "get byop_enabled" in {
    val res = get("")
    decode[Map[String, Boolean]](res).right.get should contain theSameElementsAs Map(
      "byop_enabled" -> true
    )
  }

  it should "get proxies json" in {
    val res = get("api/v4/discover_proxies")

    val json = decode[Map[String, Seq[String]]](res).right.get
    json.keys should contain theSameElementsAs Seq("proxies")
    json("proxies").nonEmpty shouldEqual true
  }

  it should "get proxies yson" in {
    import scala.collection.JavaConverters._
    val res = get("api/v4/discover_proxies", Map(
      "X-YT-Header-Format" -> YTreeTextSerializer.serialize(YtFormat.YSON_TEXT),
      "X-YT-Output-Format" -> YTreeTextSerializer.serialize(YtFormat.YSON_TEXT)
    ))

    val yson = YTreeTextSerializer.deserialize(new ByteArrayInputStream(res.getBytes(StandardCharsets.UTF_8))).asMap()
    yson.keys().asScala should contain theSameElementsAs Seq("proxies")
    yson.getOrThrow("proxies").asList().size() should be > 0
  }

  it should "be available in YtClient" in {
    val ytConf = conf.copy(
      byop = ByopConfiguration(
        enabled = true,
        ByopRemoteConfiguration(
          enabled = true,
          EmptyWorkersListStrategy.WaitAndFail(15 seconds)
        )
      ),
      masterWrapperUrl = Some(discovery.masterWrapperEndpoint().get.toString)
    )
    val wrapperClient = YtWrapper.createRpcClient(ytConf)

    wrapperClient.yt.listNode("/").join().asList().size() should be > 0
  }

  private def get(path: String, headers: Map[String, String] = Map.empty): String = {
    val getRequest = basicRequest.get(uri"http://${discovery.masterWrapperEndpoint().get}/$path")
    val requestWithHeaders = headers.foldLeft(getRequest) { case (r, (hk, hv)) => r.header(hk, hv) }

    requestWithHeaders.send().body.right.get
  }
}
