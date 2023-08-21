package tech.ytsaurus.spyt.format.batch

import org.apache.spark.sql.yson.YsonType
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.serializers.InternalRowDeserializer
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.YtWrapper.formatPath
import tech.ytsaurus.client.request.{CreateNode, WriteSerializationContext, WriteTable}
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedRowSerializer, UnversionedValue}
import tech.ytsaurus.core.cypress.{CypressNodeType, YPath}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.ysontree.YTree

import scala.collection.JavaConverters._
import scala.language.postfixOps

class HorizontalAnyBatchReaderTest extends FlatSpec with Matchers with ReadBatchRows with LocalSpark with TmpDir {

  behavior of "HorizontalBatchReaderTest"

  it should "read different kinds of any using InternalRowDeserializer" in {
    val path = YPath.simple(formatPath(tmpPath))

    // Create table.

    val ytSchema = TableSchema.fromYTree(YTree.builder().beginList()
      .beginMap()
      .key("name").value("a")
      .key("type").value("any")
      .endMap().endList().build())
    yt.createNode(new CreateNode(path, CypressNodeType.TABLE, Map(
      "schema" -> ytSchema.toYTree,
      "optimize_for" -> YTree.builder().value("scan").build(),
    ).asJava)).join()

    // Write rows.

    val rows = List(
        new UnversionedRow(List(new UnversionedValue(0, ColumnValueType.INT64, false, 42.toLong)).asJava),
        new UnversionedRow(List(new UnversionedValue(0, ColumnValueType.STRING, false, "xyz".getBytes())).asJava),
        new UnversionedRow(List(new UnversionedValue(0, ColumnValueType.ANY, false, "{}".getBytes())).asJava),
        new UnversionedRow(List(new UnversionedValue(0, ColumnValueType.NULL, false, null)).asJava),
      ).asJava
    val req = WriteTable.builder()
      .setPath(path)
      .setSerializationContext(new WriteSerializationContext(new UnversionedRowSerializer))
      .build()
    val writer = yt.writeTable(req).join()

    writer.write(rows, ytSchema)
    writer.close().join()

    // Read rows.

    val sparkSchema = SchemaConverter.sparkSchema(ytSchema.toYTree)

    val iter = YtWrapper.readTable(
      path,
      InternalRowDeserializer.getOrCreate(sparkSchema), reportBytesRead = _ => ())

    val resultRows = iter.toArray

    // Ensure proper YSON Any representations.

    resultRows(0).get(0, YsonType).asInstanceOf[Array[Byte]] should contain inOrder(0x2, 84)
    resultRows(1).get(0, YsonType).asInstanceOf[Array[Byte]] should contain inOrder(0x1, 6, 120, 121, 122)
    resultRows(2).get(0, YsonType).asInstanceOf[Array[Byte]] should contain inOrder(123, 125)
    resultRows(3).get(0, YsonType) should be (null)
  }

}
