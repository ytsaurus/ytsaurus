package ru.yandex.spark.yt.common.utils

import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.test.TestUtils

import scala.language.postfixOps

class TupleSegmentTest extends FlatSpec with Matchers
  with TestUtils with MockitoSugar with TableDrivenPropertyChecks {
  behavior of "TupleSegment"

  private val pointMinf = TuplePoint(Seq(MInfinity()))
  private val point0 = TuplePoint(Seq(RealValue(0)))
  private val point1Minf = TuplePoint(Seq(RealValue(1), MInfinity()))
  private val point5A = TuplePoint(Seq(RealValue(5), RealValue("a")))
  private val point9Z1 = TuplePoint(Seq(RealValue(9), RealValue("z"), RealValue(1.0)))
  private val pointPinfX = TuplePoint(Seq(PInfinity(), RealValue("x")))

  it should "compare tuple points" in {
    point1Minf.compare(point5A) shouldBe -1
    point5A.compare(pointPinfX) shouldBe -1
    pointPinfX.compare(point0) shouldBe 1
    point0.compare(point0) shouldBe 0
    point0.compare(point9Z1) shouldBe -1
    point9Z1.compare(pointMinf) shouldBe 1
    pointMinf.compare(point1Minf) shouldBe -1
  }

  it should "merge and union same segment sets" in {
    val segment1 = TupleSegment(point1Minf, point5A)
    val segment2 = TupleSegment(point5A, pointPinfX)

    Seq(segment1, segment2).foreach(s => AbstractSegment.intercept(Seq(Seq(s), Seq(s))) shouldBe Seq(s))
    Seq(segment1, segment2).foreach(s => AbstractSegment.union(Seq(Seq(s), Seq(s))) shouldBe Seq(s))
  }

  it should "merge" in {
    val segment1 = TupleSegment(point1Minf, point5A)
    val segment2 = TupleSegment(point5A, pointPinfX)
    val segment3 = TupleSegment(point0, point9Z1)

    AbstractSegment.intercept(Seq(segment1, segment2).map(Seq(_))) shouldBe Seq(TupleSegment(point5A, point5A))
    AbstractSegment.intercept(Seq(segment1, segment2, segment3).map(Seq(_))) shouldBe Seq(TupleSegment(point5A, point5A))
    AbstractSegment.intercept(Seq(segment2, segment3).map(Seq(_))) shouldBe Seq(TupleSegment(point5A, point9Z1))
    AbstractSegment.intercept(Seq(segment1, segment3).map(Seq(_))) shouldBe Seq(TupleSegment(point1Minf, point5A))
  }

  it should "union" in {
    val segment1 = TupleSegment(point1Minf, point5A)
    val segment2 = TupleSegment(point9Z1, pointPinfX)
    val segment3 = TupleSegment(pointMinf, point0)
    val segment4 = TupleSegment(point0, point9Z1)

    AbstractSegment.union(Seq(segment1, segment2, segment3).map(Seq(_))) shouldBe Seq(segment3, segment1, segment2)
    AbstractSegment.union(Seq(segment1, segment2).map(Seq(_))) shouldBe Seq(segment1, segment2)
    AbstractSegment.union(Seq(segment1, segment3).map(Seq(_))) shouldBe Seq(segment3, segment1)
    AbstractSegment.union(Seq(segment2, segment3, segment4).map(Seq(_))) shouldBe Seq(TupleSegment(pointMinf, pointPinfX))
    AbstractSegment.union(Seq(segment1, segment4).map(Seq(_))) shouldBe Seq(segment4)
  }

  it should "calculate coverage from segment seq" in {
    val segment1 = TupleSegment(point0, point5A)
    val segment2 = TupleSegment(point9Z1, pointPinfX)
    val segment3 = TupleSegment(point1Minf, point9Z1)
    val segment4 = TupleSegment(pointMinf, point0)

    AbstractSegment.getCovered(Seq(segment1, segment2, segment3, segment4).map(Seq(_)), 2) shouldBe Seq(
      TupleSegment(point0, point0),
      TupleSegment(point1Minf, point5A),
      TupleSegment(point9Z1, point9Z1)
    )
  }
}
