package ru.yandex.spark.yt.wrapper.dyntable

import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.{YTreeBinarySerializer, YTreeTextSerializer}
import ru.yandex.spark.yt.test.{DynTableTestUtils, LocalYtClient, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.YtWrapper.{createDynTable, createDynTableAndMount, isDynTablePrepared}
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

import java.io.ByteArrayInputStream

class YtDynTableUtilsTest extends FlatSpec with Matchers with LocalYtClient with DynTableTestUtils with TmpDir {

  behavior of "YtDynTableUtilsTest"

  it should "get empty pivotKeys" in {
    prepareTestTable(tmpPath, testData, Nil)
    YtWrapper.pivotKeys(tmpPath).map(str) should contain theSameElementsInOrderAs Seq("{}")
  }

  it should "get non empty pivotKeys" in {
    prepareTestTable(tmpPath, testData, Seq("[]", "[3]", "[6;12]"))
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

  private def str(bytes: Array[Byte]): String = {
    YTreeTextSerializer.serialize(YTreeBinarySerializer.deserialize(new ByteArrayInputStream(bytes)))
  }

}
