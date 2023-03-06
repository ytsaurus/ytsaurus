package tech.ytsaurus.spyt

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HostAndPortTest extends AnyFlatSpec with Matchers {
    behavior of "HostAndPort"

    "fromString" should "parse addresses with hostname" in {
        HostAndPort.fromString("localhost:8080") shouldEqual HostAndPort("localhost", 8080)
        a [IllegalArgumentException] should be thrownBy HostAndPort.fromString("localhost")
    }

    "fromString" should "parse ipv4 address" in {
        HostAndPort.fromString("1.2.3.4:8080") shouldEqual HostAndPort("1.2.3.4", 8080)
    }

    "fromString" should "parse ipv6 address" in {
        HostAndPort.fromString("[2001:db8::1]:80") shouldEqual HostAndPort("2001:db8::1", 80)
        HostAndPort.fromString("2001:db8::1:80") shouldEqual HostAndPort("2001:db8::1", 80)
        a [IllegalArgumentException] should be thrownBy HostAndPort.fromString("2001:db8::1:ffff")
    }
}
