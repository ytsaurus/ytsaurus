package yt.simulations

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._
import bootstrap._
import assertions._

class GetCypressRoot extends Simulation {
    val proxy = "http://n02-0001e.yt.yandex.net"

    val protocol = http
        .baseURL(proxy)
        .warmUp(proxy + "/version")
        .acceptEncodingHeader("identity")
        .shareConnections

    val headers = Map(
        "X-YT-Input-Format" -> "\"json\"",
        "X-YT-Output-Format" -> "\"json\"")

    val scn = scenario("Get Cypress Root").exec(
        http("request_0")
            .get("/api/get?path=/")
            .headers(headers)
    )

    val deadline = java.lang.Long.getLong("yt.deadline", 0)
    val till_deadline = Math.max(1L, deadline - System.currentTimeMillis / 1000).seconds

    setUp(scn.inject(
        nothingFor(till_deadline),
        rampRate(1 usersPerSec) to(200 usersPerSec) during(100 seconds),
        constantRate(200 usersPerSec) during(100 seconds),
        rampRate(200 usersPerSec) to(1 usersPerSec) during(100 seconds)
    )).protocols(protocol)
}

