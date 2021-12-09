package ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YsonTags

trait MapParser {
  self: YsonBaseReader =>

  private val endToken = YsonTags.END_MAP

  def parseYsonMapAsSparkStruct(allowEof: Boolean, schema: IndexedDataType.StructType): GenericInternalRow = {
    val keyParser = token => parseNode(token, allowEof, IndexedDataType.ScalaStringType)
    val valueParser = (token: Byte, key: Any) => {
      val field = schema.map.get(key.asInstanceOf[String])
      field match {
        case Some(meta) => parseNode(token, allowEof, meta.dataType)
        case None => parseNode(token, allowEof, IndexedDataType.NoneType)
      }
    }
    val res = new Array[Any](schema.map.size)
    readMap(endToken, allowEof)(keyParser)(valueParser) { (key, value) =>
      schema.map.get(key.asInstanceOf[String]).foreach(field =>
        res(field.index) = value
      )
    }

    new GenericInternalRow(res)
  }

  def parseYsonMapAsSparkMap(allowEof: Boolean, schema: IndexedDataType.MapType): ArrayBasedMapData = {
    val resKeys = Seq.newBuilder[Any]
    val resValues = Seq.newBuilder[Any]
    val keyParser = parseNode(_, allowEof, schema.keyType)
    val valueParser = (token: Byte, key: Any) => parseNode(token, allowEof, schema.valueType)
    readMap(endToken, allowEof)(keyParser)(valueParser) {
      (key, value) =>
        resKeys += key
        resValues += value
    }
    new ArrayBasedMapData(ArrayData.toArrayData(resKeys.result()), ArrayData.toArrayData(resValues.result()))
  }

  def parseYsonMapAsNone(endToken: Byte, allowEof: Boolean): Int = {
    readMap(endToken, allowEof) { token =>
      parseNode(token, allowEof, IndexedDataType.NoneType)
      1
    } { (token, _) =>
      parseNode(token, allowEof, IndexedDataType.NoneType)
      None
    } ((key, value) => ())
    1
  }

}
