package ru.yandex.spark.yt.format.batch

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.slf4j.LoggerFactory
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.IndexedDataType
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.spark.yt.wrapper.LogLazy
import ru.yandex.spark.yt.wrapper.table.YtArrowInputStream

import scala.collection.JavaConverters._

class ArrowBatchReader(stream: YtArrowInputStream,
                       totalRowCount: Long,
                       schema: StructType) extends BatchReaderBase(totalRowCount) with LogLazy {
  private val log = LoggerFactory.getLogger(getClass)

  private val indexedSchema = schema.fields.map(f => SchemaConverter.indexedDataType(f.dataType))

  private var _allocator: BufferAllocator = _
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
      closeReader()
      false
    }
  }

  private def closeReader(): Unit = {
    Option(_reader).foreach(_.close(false))
    Option(_allocator).foreach(_.close())
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
    log.debugLazy(s"Update arrow reader, " +
      s"allocated ${Option(_allocator).map(_.getAllocatedMemory)}, " +
      s"peak allocated ${Option(_allocator).map(_.getPeakMemoryAllocation)}")
    closeReader()

    _allocator = new RootAllocator().newChildAllocator(s"arrow reader", 0, Long.MaxValue)
    _reader = new ArrowStreamReader(stream, _allocator)
    _root = _reader.getVectorSchemaRoot
    _dictionaries = _reader.getDictionaryVectors
  }

  private def createArrowColumnVector(vector: FieldVector, dataType: IndexedDataType): ArrowColumnVector = {
    val isNullVector = vector.getNullCount == vector.getValueCount
    val dict = Option(vector.getField.getDictionary).flatMap { encoding =>
      if (_dictionaries.containsKey(encoding.getId)) {
        Some(_dictionaries.get(encoding.getId))
      } else if (!isNullVector) {
        throw new UnsupportedOperationException
      } else None
    }
    new ArrowColumnVector(dataType, vector, dict, isNullVector)
  }

  private def updateBatch(): Unit = {
    log.traceLazy(s"Read arrow batch, " +
      s"allocated ${Option(_allocator).map(_.getAllocatedMemory)}, " +
      s"peak allocated ${Option(_allocator).map(_.getPeakMemoryAllocation)}")

    _columnVectors = new Array[ColumnVector](schema.fields.length)

    val arrowSchema = _root.getSchema.getFields.asScala.map(_.getName)
    val arrowVectors = arrowSchema.zip(_root.getFieldVectors.asScala).toMap
    schema.fields.zipWithIndex.foreach { case (fieldName, index) =>
      val dataType = indexedSchema(index)
      val arrowVector = arrowVectors.get(fieldName.metadata.getString("original_name"))
        .map(createArrowColumnVector(_, dataType))
        .getOrElse(ArrowColumnVector.nullVector(dataType))
      _columnVectors(index) = arrowVector
    }
    _batch = new ColumnarBatch(_columnVectors)
  }
}
