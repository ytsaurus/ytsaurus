package ru.yandex.spark.launcher.rest

import io.circe._
import io.circe.parser._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class ByopDiscoveryServiceTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  behavior of "ByopDiscoveryTest"

  def l[A, B](a: A): Either[A, B] = Left(a)

  def lS[A](a: A): Either[A, String] = l(a)

  def lSeq[A](a: A): Either[A, Seq[String]] = l(a)

  def r[A](a: A): Either[String, A] = Right(a)

  private val hosts = Seq(
    "2a02:6b8:c0a:990:10e:ad5b:0:2334",
    "man2-4214-59f.hume.yt.gencfg-c.yandex.net"
  )

  private def worker(host: String): String =
    s"""
      |{
      |    "id" : "worker-20200525130510-2a02:6b8:c0a:990:10e:ad5b:0:2334-27003",
      |    "host" : "$host",
      |    "port" : 27003,
      |    "cores" : 20,
      |    "memory" : 81920,
      |    "endpoint" : {
      |      "traceEnabled" : false
      |    },
      |    "webUiAddress" : "http://[$host]:27004",
      |    "alive" : true
      |}
      |""".stripMargin

  private val badWorker =
    s"""
       |{
       |    "id" : "worker-20200525130510-2a02:6b8:c0a:990:10e:ad5b:0:2334-27003",
       |    "port" : 27003,
       |    "cores" : 20,
       |    "memory" : 81920,
       |    "endpoint" : {
       |      "traceEnabled" : false
       |    },
       |    "alive" : true
       |}
       |""".stripMargin

  private def masterState(workers: Seq[String]): String =
    s"""
       |{
       |  "action" : "MasterStateResponse",
       |  "serverSparkVersion" : "2.4.4",
       |  "workers" : [ ${workers.mkString(", ")} ]
       |}
       |""".stripMargin

  it should "flatten" in {
    val table = Table(
      ("seq", "expected"),
      (Seq(r("1"), r("2"), r("3")), r(Seq("1", "2", "3"))),
      (Seq(r("1"), lS("2"), r("3")), lSeq("2")),
      (Seq(lS("1")), lSeq("1")),
      (Seq.empty, Right(Seq.empty))
    )

    forAll(table) { (seq: Seq[Either[String, String]], expected: Either[String, Seq[String]]) =>
      ByopDiscoveryService.flatten(seq) shouldEqual expected
    }
  }

  it should "parseWorkerHost" in {
    def json(text: String): Json = parse(text).right.get

    hosts.foreach { host =>
      ByopDiscoveryService.parseWorkerHost(json(worker(host))) shouldEqual Right(host)
    }
    ByopDiscoveryService.parseWorkerHost(json(badWorker)).isLeft shouldEqual true
  }

  it should "parseWorkersList" in {
    val text = masterState(hosts.map(worker))
    ByopDiscoveryService.parseWorkersList(text).right.get should contain theSameElementsAs hosts
    ByopDiscoveryService.parseWorkersList(text.drop(10)).isLeft shouldEqual true
    ByopDiscoveryService.parseWorkersList(masterState(Seq(badWorker))).isLeft shouldEqual true
  }

}
