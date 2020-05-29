package ru.yandex.spark.yt.format

import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.apache.spark.sql.Encoders
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.test.{LocalSpark, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.concurrent.duration._
import scala.language.postfixOps


class DynamicTableLocalTest extends FlatSpec with Matchers with LocalSpark with TmpDir {
  "YtFileFormat" should "read dynamic table" in {
    import spark.implicits._
    val testData = (1 to 10).map(i => TestRow(i, i * 2, ('a'.toInt + i).toChar.toString))
    prepareTestTable(tmpPath, testData, Nil)
    val df = spark.read.yt(tmpPath)
    df.selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  it should "read dynamic table with long pivot keys" in {
    import spark.implicits._
    val testData = (1 to 10).map(i => TestRow(i, i * 2, ('a'.toInt + i).toChar.toString))
    prepareTestTable(tmpPath, testData, Seq("[]", "[3]", "[6;12]"))
    val df = spark.read.yt(tmpPath)
    df.selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  def prepareTestTable(path: String, data: Seq[TestRow], pivotKeys: Seq[String]): Unit = {
    val schema = Encoders.product[TestRow].schema
    YtWrapper.createTable(tmpPath, TestTableSettings(schema, isDynamic = true, sortColumns = Seq("a", "b")))
    YtWrapper.mountTable(tmpPath)
    insertRows(tmpPath, data)
    YtWrapper.unmountTable(tmpPath)
    YtWrapper.waitState(tmpPath, YtWrapper.TabletState.Unmounted, 10 seconds)
    if (pivotKeys.nonEmpty) reshardTable(path, pivotKeys)
    YtWrapper.mountTable(tmpPath)
    YtWrapper.waitState(tmpPath, YtWrapper.TabletState.Mounted, 10 seconds)
  }

  def insertRows(path: String, rows: Seq[TestRow]): Unit = {
    import scala.language.postfixOps
    import sys.process._

    implicit val encoder: Encoder[TestRow] = deriveEncoder[TestRow]
    val json = rows.map(_.asJson.noSpaces).mkString("\n")

    s"echo $json" #| s"yt --proxy localhost:8000 insert-rows --format json $path" !
  }

  def reshardTable(path: String, pivotKeys: Seq[String])(implicit yt: YtClient): Unit = {
    import scala.language.postfixOps
    import sys.process._

    s"yt --proxy localhost:8000 reshard-table $path ${pivotKeys.mkString(" ")}" !
  }
}
