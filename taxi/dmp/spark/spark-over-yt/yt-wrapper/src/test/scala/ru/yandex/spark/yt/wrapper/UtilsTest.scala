package ru.yandex.spark.yt.wrapper

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.language.postfixOps

class UtilsTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  behavior of "UtilsTest"

  it should "parseDuration" in {
    val table = Table(
      ("str", "expected"),
      ("5", 5 seconds),
      ("13s", 13 seconds),
      ("123m", 123 minutes),
      ("1min", 1 minute),
      ("305h", 305 hours),
    )

    forAll(table) { case (str: String, expected: Duration) =>
      Utils.parseDuration(str) shouldEqual expected
    }

    an [IllegalArgumentException] should be thrownBy {
      Utils.parseDuration("34t")
    }

    an [IllegalArgumentException] should be thrownBy {
      Utils.parseDuration("aaa")
    }
  }

}
