package ru.yandex.spark.yt.wrapper.cypress

import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.spark.yt.test.{LocalYtClient, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import scala.collection.JavaConverters._

class YtCypressUtilsTest extends FlatSpec with Matchers with LocalYtClient with TmpDir {
  behavior of "YtCypressUtils"

  it should "createDocument" in {
    implicit val ysonWriter: YsonWriter[TestDoc] = (t: TestDoc) => {
      new YTreeBuilder().beginMap().key("a").value(t.a).key("b").value(t.b).endMap().build()
    }

    YtWrapper.createDocument(tmpPath, new TestDoc("A", 1))

    val res = YtWrapper.readDocument(tmpPath).asMap()
    res.keys().asScala should contain theSameElementsAs Seq("a", "b")
    res.getOrThrow("a").stringValue() shouldEqual "A"
    res.getOrThrow("b").intValue() shouldEqual 1
  }

  it should "create document from case class" in {
    YtWrapper.createDocumentFromProduct(tmpPath, TestDocProduct("A", 1))

    val res = YtWrapper.readDocument(tmpPath).asMap()
    res.keys().asScala should contain theSameElementsAs Seq("a", "b")
    res.getOrThrow("a").stringValue() shouldEqual "A"
    res.getOrThrow("b").intValue() shouldEqual 1
  }

  it should "format path" in {
    YtWrapper.formatPath("ytEventLog:///home/path") shouldEqual "//home/path"
    YtWrapper.formatPath("//home/path") shouldEqual "//home/path"
    YtWrapper.formatPath("/home/path") shouldEqual "//home/path"
    YtWrapper.formatPath("ytEventLog:/home/path") shouldEqual "//home/path"
    YtWrapper.formatPath(
      YtWrapper.formatPath("ytEventLog:///home/dev/alex-shishkin/spark-test/logs/event_log_table")
    ) shouldEqual "//home/dev/alex-shishkin/spark-test/logs/event_log_table"
    an [IllegalArgumentException] should be thrownBy {
      YtWrapper.formatPath("home/path")
    }
  }

  it should "escape path" in {
    // https://yt.yandex-team.ru/docs/description/common/ypath#simple_ypath_lexis
    val unescaped = "\\a/b@c&d*e[f{g"
    val escaped = YtWrapper.escape(unescaped)
    escaped shouldEqual "\\\\a\\/b\\@c\\&d\\*e\\[f\\{g"
  }
}

class TestDoc(val a: String, val b: Int)

case class TestDocProduct(a: String, b: Int)
