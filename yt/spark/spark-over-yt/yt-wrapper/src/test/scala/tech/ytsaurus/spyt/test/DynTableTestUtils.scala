package tech.ytsaurus.spyt.test

import tech.ytsaurus.spyt.wrapper.YtWrapper.{createTable, insertRows, mountTableSync, reshardTable, unmountTableSync}
import tech.ytsaurus.spyt.wrapper.table.YtTableSettings
import tech.ytsaurus.core.tables.{ColumnSchema, ColumnSortOrder, ColumnValueType, TableSchema}
import tech.ytsaurus.ysontree.YTreeNode

import scala.concurrent.duration._
import scala.language.postfixOps

trait DynTableTestUtils {
  self: LocalYtClient =>

  val testSchema: TableSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .add(new ColumnSchema("a", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
    .add(new ColumnSchema("b", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
    .addValue("c", ColumnValueType.STRING)
    .build()

  val orderedTestSchema: TableSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("a", ColumnValueType.INT64)
    .addValue("b", ColumnValueType.INT64)
    .addValue("c", ColumnValueType.STRING)
    .build()

  val testData: Seq[TestRow] = getTestData()
  val testRow: TestRow = TestRow(100, 100, "new_row")

  def getTestData(low: Int = 1, high: Int = 10): Seq[TestRow] = {
    (low to high).map(i => TestRow(i, i * 2, ('a'.toInt + i).toChar.toString))
  }

  def getTestSchema(sorted: Boolean = true): TableSchema = if (sorted) testSchema else orderedTestSchema

  def prepareTestTable(path: String, data: Seq[TestRow], pivotKeys: Seq[Seq[Any]],
                       enableDynamicStoreRead: Boolean = false): Unit = {
    val options = Map("enable_dynamic_store_read" -> enableDynamicStoreRead.toString)
    val schema = getTestSchema()
    createTable(path, TestTableSettings(schema.toYTree, isDynamic = true, sortColumns = Seq("a", "b"), options))
    mountTableSync(path, 10 seconds)
    insertRows(path, schema, data.map(r => r.productIterator.toList))
    unmountTableSync(path, 10 seconds)
    if (pivotKeys.nonEmpty) reshardTable(path, schema, pivotKeys)
    mountTableSync(path, 10 seconds)
  }

  def prepareOrderedTestTable(path: String, enableDynamicStoreRead: Boolean = false,
                              tabletCount: Int = 3): Unit = {
    val options =
      Map("enable_dynamic_store_read" -> enableDynamicStoreRead.toString, "tablet_count" -> tabletCount)
    val schema = getTestSchema(sorted = false)
    createTable(path, TestTableSettings(schema.toYTree, isDynamic = true, sortColumns = Nil, options))
    mountTableSync(path, 10 seconds)
  }

  def appendChunksToTestTable(path: String, data: Seq[Seq[TestRow]], sorted: Boolean = true): Unit = {
    val schema = getTestSchema(sorted)
    data.foreach(chunk => insertRows(path, schema, chunk.map(r => r.productIterator.toList)))
    unmountTableSync(path, 10 seconds)
    mountTableSync(path, 10 seconds)
  }
}

case class TestTableSettings(ytSchema: YTreeNode,
                             isDynamic: Boolean = false,
                             sortColumns: Seq[String] = Nil,
                             otherOptions: Map[String, Any] = Map.empty) extends YtTableSettings {
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