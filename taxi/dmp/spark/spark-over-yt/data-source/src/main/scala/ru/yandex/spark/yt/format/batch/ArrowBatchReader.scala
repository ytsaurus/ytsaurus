package ru.yandex.spark.yt.format.batch

import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.{VarBinaryVector, VectorSchemaRoot}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.spark.yt.wrapper.table.YtArrowInputStream

import scala.collection.JavaConverters._
import scala.collection.mutable

class ArrowBatchReader(stream: YtArrowInputStream,
                       totalRowCount: Long,
                       schema: StructType) extends BatchReaderBase(totalRowCount) {
  private val log = Logger.getLogger(getClass)

  private val indexedSchema = schema.fields.map(f => SchemaConverter.indexedDataType(f.dataType))

  private val allocator = ArrowUtils.rootAllocator.newChildAllocator(s"stdin reader", 0, Long.MaxValue)
  private var _reader: ArrowStreamReader = _
  private var _root: VectorSchemaRoot = _
  private var _dictionaries: java.util.Map[java.lang.Long, Dictionary] = _
  private var _columnVectors: Array[ColumnVector] = _

  updateReader()
  updateBatch()

  override protected def nextBatchInternal: Boolean = {
    if (stream.isNextPage) updateReader()
    val batchLoaded = _reader.loadNextBatch()
    if (batchLoaded) {
      updateBatch()
      setNumRows(_root.getRowCount)
      true
    } else {
      _reader.close(false)
      allocator.close()
      false
    }
  }

  override protected def finalRead(): Unit = {
    val bytes = new Array[Byte](9)
    val res = stream.read(bytes)
    val isAllowedBytes = bytes.forall(_ == 0) || (bytes.take(4).forall(_ == -1) && bytes.drop(4).forall(_ == 0))
    if (res > 8 || !isAllowedBytes) {
      throw new IllegalStateException()
    }
  }

  override def close(): Unit = {
    stream.close()
  }

  private def updateReader(): Unit = {
    _reader = new ArrowStreamReader(stream, allocator)
    _root = _reader.getVectorSchemaRoot
    _dictionaries = _reader.getDictionaryVectors
  }

  private def updateBatch(): Unit = {
    _columnVectors = _root.getFieldVectors.asScala.zip(indexedSchema).map { case (vector, dt) =>
      val isNullVector = vector.getNullCount == vector.getValueCount
      val dict = Option(vector.getField.getDictionary).flatMap { encoding =>
        if (_dictionaries.containsKey(encoding.getId)) {
          Some(_dictionaries.get(encoding.getId))
        } else if (!isNullVector) {
          throw new UnsupportedOperationException
        } else None
      }
      new ArrowColumnVector(dt, vector, dict, isNullVector)
    }.toArray[ColumnVector]
    _batch = new ColumnarBatch(_columnVectors)
  }
}
