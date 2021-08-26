package ru.yandex.spark.yt.wrapper

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.wrapper.Utils.flatten

import scala.concurrent.duration._
import scala.language.postfixOps

class UtilsTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {
  behavior of "UtilsTest"

  def l[A, B](a: A): Either[A, B] = Left(a)

  def lS[A](a: A): Either[A, String] = l(a)

  def lSeq[A](a: A): Either[A, Seq[String]] = l(a)

  def r[A](a: A): Either[String, A] = Right(a)

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

  it should "flatten" in {
    val table = Table(
      ("seq", "expected"),
      (Seq(r("1"), r("2"), r("3")), r(Seq("1", "2", "3"))),
      (Seq(r("1"), lS("2"), r("3")), lSeq("2")),
      (Seq(lS("1")), lSeq("1")),
      (Seq.empty, Right(Seq.empty))
    )

    forAll(table) { (seq: Seq[Either[String, String]], expected: Either[String, Seq[String]]) =>
      flatten(seq) shouldEqual expected
    }
  }
}
