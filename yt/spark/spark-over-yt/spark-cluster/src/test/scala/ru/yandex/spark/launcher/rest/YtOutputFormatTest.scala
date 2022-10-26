package ru.yandex.spark.launcher.rest

import org.scalatest.{FlatSpec, Matchers}

class YtOutputFormatTest extends FlatSpec with Matchers {

  behavior of "YtOutputFormatTest"

  it should "format discovery info to json" in {
    val info = DiscoveryInfo(Seq("a:1", "b:2"))
    YtOutputFormat.Json.format(info) shouldEqual """{"proxies":["a:1","b:2"]}""".stripMargin
  }

  it should "format discovery info to yson" in {
    val info = DiscoveryInfo(Seq("a:1", "b:2"))
    YtOutputFormat.Yson.format(info) shouldEqual """{"proxies"=["a:1";"b:2"]}"""
  }
}
