package ru.yandex.spark.yt.format

import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.bolts.collection.MapF
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.test.{LocalSpark, TmpDir}
import ru.yandex.spark.yt.wrapper.{YtJavaConverters, YtWrapper}
import ru.yandex.spark.yt._

import scala.collection.JavaConverters._

class YtSortedTablesTest extends FlatSpec with Matchers with LocalSpark with TmpDir {
  import spark.implicits._

  "YtFileFormat" should "write sorted table" in {
    val df = (1 to 9).toDF.coalesce(3)

    df.write.sortedBy("value").yt(tmpPath)

    sortColumns(tmpPath) should contain theSameElementsAs Seq("value")
    ytSchema(tmpPath) should contain theSameElementsAs Seq(
      Map("name" -> Some("value"), "type" -> Some("int32"), "sort_order" -> Some("ascending"))
    )
  }

  it should "change columns order in sorted table" in {
    val df = (1 to 9).zip(9 to 1 by -1).toDF("a", "b")

    spark.conf.set("spark.sql.adaptive.enabled", "false")
    df.sort("b").coalesce(3).write.sortedBy("b").yt(tmpPath)

    sortColumns(tmpPath) should contain theSameElementsAs Seq("b")
    ytSchema(tmpPath) should contain theSameElementsAs Seq(
      Map("name" -> Some("b"), "type" -> Some("int32"), "sort_order" -> Some("ascending")),
      Map("name" -> Some("a"), "type" -> Some("int32"), "sort_order" -> None)
    )
  }

  it should "abort transaction if failed to create sorted table" in {
    val df = (1 to 9).toDF("my_name").coalesce(3)
    an[Exception] shouldBe thrownBy {
      df.write.sortedBy("bad_name").yt(tmpPath)
    }
    noException shouldBe thrownBy {
      df.write.sortedBy("my_name").yt(tmpPath)
    }
  }

  def sortColumns(path: String): Seq[String] = {
    YtWrapper.attribute(path, "sorted_by").asList().asScala.map(_.stringValue())
  }

  def option(map: MapF[String, YTreeNode], name: String): Option[String] = {
    YtJavaConverters.toOption(map.getOptional(name)).map(_.stringValue())
  }

  def ytSchema(path: String): Seq[Map[String, Option[String]]] = {
    val schemaFields = Seq("name", "type", "sort_order")
    YtWrapper.attribute(path, "schema").asList().asScala.map { field =>
      val map = field.asMap()
      schemaFields.map(n => n -> option(map, n)).toMap
    }
  }

}
