package org.apache.spark.deploy.rest

import org.apache.spark.deploy.rest.MasterClient.{parseDriversList, parseWorkersList}
import org.scalatest.{FlatSpec, Matchers}

class MasterClientTest extends FlatSpec with Matchers {
  behavior of "MasterClient"

  it should "parse json with drivers list" in {
    val drivers = List(
      DriverInfo(0, "1"),
      DriverInfo(0, "2"),
      DriverInfo(0, "4"),
    )
    val json = """{"drivers": [""" +
      drivers.map(x => s"""{"id": "${x.id}", "startTime": ${x.startTime}}""").mkString(", ") +
      "]}"
    val res = parseDriversList(json)
    res.right.get should contain theSameElementsAs drivers.map(x => x.id)
  }

  it should "parse json with workers list" in {
    val workers = List(
      WorkerInfo("worker-1", "host1.yandex.net", 27003, 4, 4096, "http://host1.yandex.net:27004", alive = true,
        Map("driverop" -> ResourceInfo("driverop", Seq("1", "2", "3")))),
      WorkerInfo("worker-2", "host2.yandex.net", 27003, 4, 4096, "http://host2.yandex.net:27004", alive = true,
        Map()),
      WorkerInfo("worker-3", "host3.yandex.net", 27003, 4, 4096, "http://host3.yandex.net:27004", alive = false,
        Map())
    )
    val json =
      """
        |{
        |  "action" : "MasterStateResponse",
        |  "serverSparkVersion" : "3.0.1",
        |  "workers" : [ {
        |    "id" : "worker-1",
        |    "host" : "host1.yandex.net",
        |    "port" : 27003,
        |    "cores" : 4,
        |    "memory" : 4096,
        |    "endpoint" : {
        |      "traceEnabled" : false
        |    },
        |    "webUiAddress" : "http://host1.yandex.net:27004",
        |    "resources" : {
        |      "driverop" : {
        |        "name" : "driverop",
        |        "addresses" : [ "1", "2", "3"]
        |      }
        |    },
        |    "alive" : true
        |  }, {
        |    "id" : "worker-2",
        |    "host" : "host2.yandex.net",
        |    "port" : 27003,
        |    "cores" : 4,
        |    "memory" : 4096,
        |    "endpoint" : {
        |      "traceEnabled" : false
        |    },
        |    "webUiAddress" : "http://host2.yandex.net:27004",
        |    "resources" : { },
        |    "alive" : true
        |  }, {
        |    "id" : "worker-3",
        |    "host" : "host3.yandex.net",
        |    "port" : 27003,
        |    "cores" : 4,
        |    "memory" : 4096,
        |    "endpoint" : {
        |      "traceEnabled" : false
        |    },
        |    "webUiAddress" : "http://host3.yandex.net:27004",
        |    "resources" : { },
        |    "alive" : false
        |  } ]
        }
        """.stripMargin
    val res = parseWorkersList(json)
    res shouldBe Right(workers)
  }
}
