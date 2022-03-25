package ru.yandex.spark.yt.test

import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.YtWrapper.{createTable, insertRows, mountTableSync, reshardTable, unmountTableSync}
import ru.yandex.spark.yt.wrapper.table.YtTableSettings
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.tables.{ColumnSchema, ColumnSortOrder, ColumnValueType, TableSchema}

import scala.concurrent.duration._
import scala.language.postfixOps

trait DynTableTestUtils {
  self: LocalYtClient =>

  val testSchema: TableSchema = new TableSchema.Builder()
    .setUniqueKeys(false)
    .add(new ColumnSchema("a", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
    .add(new ColumnSchema("b", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
    .addValue("c", ColumnValueType.STRING)
    .build()
  private val testSchemaYt = testSchema.toYTree

  val testData: Seq[TestRow] = (1 to 10).map(i => TestRow(i, i * 2, ('a'.toInt + i).toChar.toString))
  val testRow: TestRow = TestRow(100, 100, "new_row")

  def prepareTestTable(path: String, data: Seq[TestRow], pivotKeys: Seq[Seq[Any]]): Unit = {
    createTable(path, TestTableSettings(testSchemaYt, isDynamic = true, sortColumns = Seq("a", "b")))
    mountTableSync(path, 10 seconds)
    insertRows(path, testSchema, data.map(r => r.productIterator.toList))
    unmountTableSync(path, 10 seconds)
    if (pivotKeys.nonEmpty) reshardTable(path, testSchema, pivotKeys)
    mountTableSync(path, 10 seconds)
  }
}

case class TestTableSettings(ytSchema: YTreeNode,
                             isDynamic: Boolean = false,
                             sortColumns: Seq[String] = Nil,
                             otherOptions: Map[String, String] = Map.empty) extends YtTableSettings {
  override def optionsAny: Map[String, Any] = otherOptions + ("dynamic" -> isDynamic)
}

object TestTableSettings {
  def apply(schema: TableSchema, isDynamic: Boolean): YtTableSettings = {
    import scala.collection.JavaConverters._
    val keyColumns = schema.getColumns.asScala.filter(_.getSortOrder != null).map(_.getName)
    TestTableSettings(schema.toYTree, isDynamic, sortColumns = keyColumns)
  }
}

case class TestRow(a: Long, b: Long, c: String)