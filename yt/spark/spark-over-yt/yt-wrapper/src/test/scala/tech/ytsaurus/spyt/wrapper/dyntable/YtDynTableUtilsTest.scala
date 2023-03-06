package tech.ytsaurus.spyt.wrapper.dyntable

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalYtClient, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.YtWrapper.{countRows, createDynTable, createDynTableAndMount, insertRows, isDynTablePrepared}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.ysontree.{YTreeBinarySerializer, YTreeTextSerializer}

import java.io.ByteArrayInputStream

//noinspection ZeppelinScalaResUsageFilter
class YtDynTableUtilsTest extends AnyFlatSpec with Matchers with LocalYtClient with DynTableTestUtils with TmpDir {

  behavior of "YtDynTableUtilsTest"

  it should "get empty pivotKeys" in {
    prepareTestTable(tmpPath, testData, Nil)
    YtWrapper.pivotKeys(tmpPath).map(str) should contain theSameElementsInOrderAs Seq("[]")
  }

  it should "get non empty pivotKeys" in {
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)))
    YtWrapper.pivotKeys(tmpPath).map(str) should contain theSameElementsInOrderAs Seq(
      "[]", """[3;]""", """[6;12;]"""
    )
  }

  it should "createDynTableAndMount" in {
    val schema = TableSchema.builder().setUniqueKeys(true)
      .addKey("mockKey", ColumnValueType.INT64)
      .addValue("mockValue", ColumnValueType.INT64).build()

    isDynTablePrepared(tmpPath) shouldEqual false

    createDynTable(tmpPath, schema)
    isDynTablePrepared(tmpPath) shouldEqual false

    createDynTableAndMount(tmpPath, schema)
    isDynTablePrepared(tmpPath) shouldEqual true

    createDynTableAndMount(tmpPath, schema)
    isDynTablePrepared(tmpPath) shouldEqual true

    a[RuntimeException] shouldBe thrownBy {
      createDynTableAndMount(tmpPath, schema, ignoreExisting = false)
    }
    isDynTablePrepared(tmpPath) shouldEqual true
  }

  it should "count rows" in {
    val schema = TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("value", ColumnValueType.INT64).build()
    createDynTableAndMount(tmpPath, schema)

    val data = 0 until 20
    insertRows(tmpPath, schema, data.map(Seq(_)))

    val res1 = countRows(tmpPath)
    res1 shouldBe 20
    val res2 = countRows(tmpPath, Some(s"""5 <= value and value < 12"""))
    res2 shouldBe 7
    val res3 = countRows(tmpPath, Some(s"""value % 2 = 0"""))
    res3 shouldBe 10
  }

  private def str(bytes: Array[Byte]): String = {
    YTreeTextSerializer.serialize(YTreeBinarySerializer.deserialize(new ByteArrayInputStream(bytes)))
  }
}
