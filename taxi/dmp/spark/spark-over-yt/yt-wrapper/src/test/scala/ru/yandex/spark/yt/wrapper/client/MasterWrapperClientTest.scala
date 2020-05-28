package ru.yandex.spark.yt.wrapper.client

import org.scalatest.{FlatSpec, Matchers}

class MasterWrapperClientTest extends FlatSpec with Matchers {

  behavior of "ByopDiscoveryClientTest"

  it should "parseByopEnabled" in {
    def text(enabled: Boolean) =
      s"""
        |{
        |"byop_enabled": $enabled
        |}
        |""".stripMargin

    MasterWrapperClient.parseByopEnabled(text(true)) shouldEqual Right(true)
    MasterWrapperClient.parseByopEnabled(text(false)) shouldEqual Right(false)
    MasterWrapperClient.parseByopEnabled("{}").isLeft shouldEqual true
    MasterWrapperClient.parseByopEnabled("}").isLeft shouldEqual true
  }

}
