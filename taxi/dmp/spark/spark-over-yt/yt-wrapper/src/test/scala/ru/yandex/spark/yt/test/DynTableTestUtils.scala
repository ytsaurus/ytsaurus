package ru.yandex.spark.yt.test

import io.circe._
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.table.YtTableSettings
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.tables.{ColumnSchema, ColumnSortOrder, ColumnValueType, TableSchema}

import scala.concurrent.duration._
import scala.language.postfixOps

trait DynTableTestUtils {
  self: LocalYtClient =>

  private val testSchema = new TableSchema.Builder()
    .setUniqueKeys(false)
    .add(new ColumnSchema("a", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
    .add(new ColumnSchema("b", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
    .addValue("c", ColumnValueType.STRING)
    .build().toYTree

  def testData: Seq[TestRow] = (1 to 10).map(i => TestRow(i, i * 2, ('a'.toInt + i).toChar.toString))

  def prepareTestTable(path: String, data: Seq[TestRow], pivotKeys: Seq[String]): Unit = {
    YtWrapper.createTable(path, TestTableSettings(testSchema, isDynamic = true, sortColumns = Seq("a", "b")))
    YtWrapper.mountTable(path)
    insertRows(path, data)
    YtWrapper.unmountTable(path)
    YtWrapper.waitState(path, YtWrapper.TabletState.Unmounted, 10 seconds)
    if (pivotKeys.nonEmpty) reshardTable(path, pivotKeys)
    YtWrapper.mountTable(path)
    YtWrapper.waitState(path, YtWrapper.TabletState.Mounted, 10 seconds)
  }

  def insertRows(path: String, rows: Seq[TestRow]): Unit = {
    import scala.language.postfixOps
    import sys.process._

    implicit val encoder: Encoder[TestRow] = deriveEncoder[TestRow]
    val json = rows.map(_.asJson.noSpaces).mkString("\n")

    s"echo $json" #| s"yt --proxy localhost:8000 insert-rows --format json $path" !
  }

  def reshardTable(path: String, pivotKeys: Seq[String])(implicit yt: CompoundClient): Unit = {
    import scala.language.postfixOps
    import sys.process._

    s"yt --proxy localhost:8000 reshard-table $path ${pivotKeys.mkString(" ")}" !
  }

}

case class TestTableSettings(ytSchema: YTreeNode,
                             isDynamic: Boolean = false,
                             sortColumns: Seq[String] = Nil,
                             otherOptions: Map[String, String] = Map.empty) extends YtTableSettings {
  override def optionsAny: Map[String, Any] = otherOptions + ("dynamic" -> isDynamic)
}

case class TestRow(a: Long, b: Long, c: String)