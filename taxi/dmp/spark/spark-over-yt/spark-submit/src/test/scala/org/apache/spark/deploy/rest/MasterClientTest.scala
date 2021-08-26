package org.apache.spark.deploy.rest

import org.apache.spark.deploy.rest.MasterClient.parseDriversList
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
}
