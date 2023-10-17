package tech.ytsaurus.spyt.wrapper.client

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.Duration

class YtClientConfigurationTest extends AnyFlatSpec with Matchers {

  private val baseConf =
    YtClientConfiguration("proxy", "user", "token", Duration.Zero, None, null, None, extendedFileTimeout = false)

  "YtClientConfiguration" should "parse short proxy" in {
    val conf = baseConf.copy(proxy = "short")

    conf.fullProxy shouldEqual "short.yt.yandex.net"
    conf.port shouldEqual 80
    conf.isHttps shouldBe false
  }

  it should "parse long proxy" in {
    val conf = baseConf.copy(proxy = "name.yt.yandex.net")

    conf.fullProxy shouldEqual "name.yt.yandex.net"
    conf.port shouldEqual 80
    conf.isHttps shouldBe false
  }

  it should "parse long proxy with port" in {
    val conf = baseConf.copy(proxy = "name.yt.yandex.net:8082")

    conf.fullProxy shouldEqual "name.yt.yandex.net"
    conf.port shouldEqual 8082
    conf.isHttps shouldBe false
  }

  it should "parse proxy with schema" in {
    val conf = baseConf.copy(proxy = "https://some.random.proxy.net")

    conf.fullProxy shouldEqual "some.random.proxy.net"
    conf.port shouldEqual 443
    conf.isHttps shouldBe true
  }

  it should "parse proxy with schema and port" in {
    val conf = baseConf.copy(proxy = "https://some.random.proxy.net:8082/")

    conf.fullProxy shouldEqual "some.random.proxy.net"
    conf.port shouldEqual 8082
    conf.isHttps shouldBe true
  }

  it should "parse proxy with schema, ipv4 address and port" in {
    val conf = baseConf.copy(proxy = "https://127.0.0.1:8082/")

    conf.fullProxy shouldEqual "127.0.0.1"
    conf.port shouldEqual 8082
    conf.isHttps shouldBe true
  }

  it should "parse proxy with schema, ipv6 address and port" in {
    val conf = baseConf.copy(proxy = "https://[::1]:8082/")

    conf.fullProxy shouldEqual "[::1]"
    conf.port shouldEqual 8082
    conf.isHttps shouldBe true
  }
}
