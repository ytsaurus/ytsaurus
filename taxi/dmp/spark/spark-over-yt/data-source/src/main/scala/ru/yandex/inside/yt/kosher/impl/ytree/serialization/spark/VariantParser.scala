package ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.IntegerType
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YsonTags

trait VariantParser {
  self: YsonBaseReader =>

  private val endToken = YsonTags.END_LIST

  private def parseYsonVariantAsArray(allowEof: Boolean, count: Int,
                                      getTypeByIndex: Int => IndexedDataType, getIndexByKey: String => Int): Array[Any] = {
    val res = new Array[Any](count)
    val first = parseToken(allowEof)
    val (index, dt) = first match {
      case YsonTags.BINARY_INT =>
        val index = parseNode(first, allowEof, IndexedDataType.AtomicType(IntegerType)).asInstanceOf[Int]
        (index, getTypeByIndex(index))
      case YsonTags.BINARY_STRING =>
        val key = parseNode(first, allowEof, IndexedDataType.ScalaStringType).asInstanceOf[String]
        val index = getIndexByKey(key)
        (index, getTypeByIndex(index))
    }

    val separator = parseToken(allowEof)
    if (separator != YsonTags.ITEM_SEPARATOR) unexpectedToken(separator, "ITEM_SEPARATOR")

    res(index) = parseNode(parseToken(allowEof), allowEof, dt)

    val end = parseToken(allowEof)
    if (end == YsonTags.ITEM_SEPARATOR) {
      val next = parseToken(allowEof)
      if (next != endToken) unexpectedToken(next, "END_TOKEN")
    } else {
      if (end != endToken) unexpectedToken(end, "END_TOKEN")
    }

    res
  }

  def parseYsonVariantAsSparkTuple(allowEof: Boolean, schema: IndexedDataType.VariantOverTupleType): InternalRow = {
    new GenericInternalRow(parseYsonVariantAsArray(allowEof, schema.inner.length,
      schema.inner.apply, _ => throw new UnsupportedOperationException))
  }

  def parseYsonVariantAsSparkStruct(allowEof: Boolean, schema: IndexedDataType.VariantOverStructType): InternalRow = {
    new GenericInternalRow(parseYsonVariantAsArray(allowEof, schema.inner.map.size,
      i => schema.inner.map(schema.inner.sparkDataType.fields(i).name).dataType, k => schema.inner.map(k).index))
  }

}
