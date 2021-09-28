package ru.yandex.spark.launcher.rest

import io.circe._
import io.circe.parser._
import org.scalatest.{FlatSpec, Matchers}

class ByopDiscoveryServiceTest extends FlatSpec with Matchers {
  behavior of "ByopDiscoveryTest"

  private val hosts = Seq(
    "2a02:6b8:c0a:990:10e:ad5b:0:2334",
    "man2-4214-59f.hume.yt.gencfg-c.yandex.net"
  )

  private def worker(host: String, alive: Boolean): String =
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
      |    "alive" : $alive
      |}
      |""".stripMargin

  private val workerWithoutHost =
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

  private val workerWithoutAlive =
    s"""
       |{
       |    "id" : "worker-20200525130510-2a02:6b8:c0a:990:10e:ad5b:0:2334-27003",
       |    "host" : "man2-4214-59f.hume.yt.gencfg-c.yandex.net",
       |    "port" : 27003,
       |    "cores" : 20,
       |    "memory" : 81920,
       |    "endpoint" : {
       |      "traceEnabled" : false
       |    }
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

  it should "parseWorkerInfo" in {
    def json(text: String): Json = parse(text).right.get

    for {
      host <- hosts
      alive <- Seq(true, false)
    } {
      ByopDiscoveryService.parseWorkerInfo(json(worker(host, alive))) shouldEqual Right(WorkerInfo(host, alive))
    }

    ByopDiscoveryService.parseWorkerInfo(json(workerWithoutAlive)).isLeft shouldEqual true
    ByopDiscoveryService.parseWorkerInfo(json(workerWithoutHost)).isLeft shouldEqual true
  }

  it should "parseWorkersList" in {
    val alive = Seq(false, true)
    val workerInfos = hosts.zip(alive).map{case (h, a) => WorkerInfo(h, a)}
    val text = masterState(workerInfos.map(i => worker(i.host, i.alive)))

    ByopDiscoveryService.parseWorkersList(text).right.get should contain theSameElementsAs workerInfos
    ByopDiscoveryService.parseWorkersList(text.drop(10)).isLeft shouldEqual true
    ByopDiscoveryService.parseWorkersList(masterState(Seq(workerWithoutHost))).isLeft shouldEqual true
    ByopDiscoveryService.parseWorkersList(masterState(Seq(workerWithoutAlive))).isLeft shouldEqual true
  }

  it should "convert workers infos to proxies list" in {
    val alive = Seq(false, true)
    val workerInfos = hosts.zip(alive).map{case (h, a) => WorkerInfo(h, a)}

    val proxies = ByopDiscoveryService.workersToProxies(workerInfos, 27002)
    proxies should contain theSameElementsAs Seq(s"${hosts(1)}:27002")
  }
}
