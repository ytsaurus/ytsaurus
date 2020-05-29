package ru.yandex.spark.yt.wrapper.dyntable

import java.io.ByteArrayInputStream

import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.{YTreeBinarySerializer, YTreeTextSerializer}
import ru.yandex.spark.yt.test.{DynTableTestUtils, LocalYtClient, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper

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

  private def str(bytes: Array[Byte]): String = {
    YTreeTextSerializer.serialize(YTreeBinarySerializer.deserialize(new ByteArrayInputStream(bytes)))
  }

}
