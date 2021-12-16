package ru.yandex.spark.yt.wrapper.dyntable

import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.{YTreeBinarySerializer, YTreeTextSerializer}
import ru.yandex.spark.yt.test.{DynTableTestUtils, LocalYtClient, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.YtWrapper.{countRows, createDynTable, createDynTableAndMount, insertRows, isDynTablePrepared, selectRows}
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

import java.io.ByteArrayInputStream

class YtDynTableUtilsTest extends FlatSpec with Matchers with LocalYtClient with DynTableTestUtils with TmpDir {

  behavior of "YtDynTableUtilsTest"

  it should "get empty pivotKeys" in {
    prepareTestTable(tmpPath, testData, Nil)
    YtWrapper.pivotKeys(tmpPath).map(str) should contain theSameElementsInOrderAs Seq("{}")
  }

  it should "get non empty pivotKeys" in {
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)))
    YtWrapper.pivotKeys(tmpPath).map(str) should contain theSameElementsInOrderAs Seq(
      "{}", """{"a"=3}""", """{"a"=6;"b"=12}"""
    )
  }

  it should "createDynTableAndMount" in {
    val schema = TableSchema.builder()
      .addKey("mockKey", ColumnValueType.INT64)
      .addValue("mockValue", ColumnValueType.INT64).build()

    isDynTablePrepared(tmpPath) shouldEqual false

    createDynTable(tmpPath, schema)
    isDynTablePrepared(tmpPath) shouldEqual false

    createDynTableAndMount(tmpPath, schema)
    isDynTablePrepared(tmpPath) shouldEqual true

    createDynTableAndMount(tmpPath, schema, ignoreExisting = true)
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
