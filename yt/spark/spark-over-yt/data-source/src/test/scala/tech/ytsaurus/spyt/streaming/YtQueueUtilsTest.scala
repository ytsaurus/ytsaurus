package tech.ytsaurus.spyt.streaming

import org.apache.spark.sql.execution.streaming.SerializedOffset
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.SortedMap

class YtQueueUtilsTest extends FlatSpec with Matchers {
  it should "throw exception when partition list is sparse" in {
    YtQueueOffset("", "", SortedMap(0 -> 0L, 1 -> 1L, 2 -> 2L, 3 -> 3L))
    a[IllegalArgumentException] should be thrownBy {
      YtQueueOffset("", "", SortedMap(0 -> 0L, 1 -> 1L, 2 -> 2L, 4 -> 3L))
    }
  }

  it should "get ranges" in {
    val offset1 = YtQueueOffset("cluster", "//path", SortedMap(0 -> 0L, 1 -> 0L))
    val offset2 = YtQueueOffset("cluster", "//path", SortedMap(0 -> 0L, 1 -> 1L, 2 -> 2L))
    val offset3 = YtQueueOffset("cluster", "//path", SortedMap(0 -> 2L, 1 -> 1L, 2 -> 7L))
    YtQueueOffset.getRanges(offset1, offset3) should contain theSameElementsAs Seq(
      YtQueueRange(0, 1L, 3L),
      YtQueueRange(1, 1L, 2L),
      YtQueueRange(2, 0L, 8L),
    )
    YtQueueOffset.getRanges(offset2, offset3) should contain theSameElementsAs Seq(
      YtQueueRange(0, 1L, 3L),
      YtQueueRange(1, 2L, 2L),
      YtQueueRange(2, 3L, 8L),
    )
  }

  it should "deserialize offset from json" in {
    val jsonOffset = SerializedOffset("""{"cluster":"c","path":"p","partitions":{"0":2,"1":1,"2":7}}""")
    YtQueueOffset.apply(jsonOffset) shouldBe YtQueueOffset("c", "p", SortedMap(0 -> 2L, 1 -> 1L, 2 -> 7L))
  }

  it should "be serializable to json" in {
    val offset1 = YtQueueOffset("c2", "p0", SortedMap(0 -> 100L, 1 -> 10L, 2 -> 75L, 3 -> 10L, 4 -> 200L))
    val offset2 = YtQueueOffset("cluster", "//home/path", SortedMap(0 -> 1L, 1 -> 5L, 2 -> 7L, 3 -> 0L, 4 -> 17L, 5 -> 0L))
    Seq(offset1, offset2).foreach(offset =>
      YtQueueOffset.apply(SerializedOffset(offset.json())) shouldBe offset
    )
  }

  it should "serialize to single line" in {
    val offset1 = YtQueueOffset("c2", "p0", SortedMap(0 -> 100L, 1 -> 10L, 2 -> 75L, 3 -> 10L, 4 -> 200L))
    offset1.json().indexOf('\n') shouldBe -1
  }

  it should "serialize with sorted partitions" in {
    val offset1 = YtQueueOffset("", "", SortedMap(2 -> 100L, 1 -> 10L, 0 -> 75L, 3 -> 10L, 4 -> 101L))
    offset1.json() shouldBe """{"cluster":"","path":"","partitions":{"0":75,"1":10,"2":100,"3":10,"4":101}}"""
    val offset2 = YtQueueOffset("", "", SortedMap(1 -> 1L, 0 -> 5L, 2 -> 7L))
    offset2.json() shouldBe """{"cluster":"","path":"","partitions":{"0":5,"1":1,"2":7}}"""
    val offset3 = YtQueueOffset("", "", SortedMap(0 -> 0L, 1 -> 1L))
    offset3.json() shouldBe """{"cluster":"","path":"","partitions":{"0":0,"1":1}}"""
  }

  it should "get max safely" in {
    YtQueueOffset.getSafeMax(Seq(1, 2, 0)) shouldBe Some(2)
    YtQueueOffset.getSafeMax(Seq.empty[String]) shouldBe None
    YtQueueOffset.getSafeMax(Seq("a", "bacaba")) shouldBe Some("bacaba")
  }
}
