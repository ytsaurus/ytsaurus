package ru.yandex.spark.yt.serializers

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.spark.metrics.yt.YtMetricsRegister
import org.apache.spark.metrics.yt.YtMetricsRegister.ytMetricsSource._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.yson.YsonType
import org.slf4j.LoggerFactory
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.YsonEncoder
import ru.yandex.spark.yt.wrapper.LogLazy
import ru.yandex.yt.ytclient.`object`.{WireProtocolWriteable, WireRowSerializer}
import ru.yandex.yt.ytclient.proxy.TableWriter
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

class InternalRowSerializer(schema: StructType, schemaHint: Map[String, YtLogicalType])
  extends WireRowSerializer[InternalRow] with LogLazy {

  private val log = LoggerFactory.getLogger(getClass)

  private val tableSchema = SchemaConverter.tableSchema(schema, Nil, schemaHint)

  override def getSchema: TableSchema = tableSchema

  private def valueId(id: Int, idMapping: Array[Int]): Int = {
    if (idMapping != null) {
      idMapping(id)
    } else id
  }

  private def writeHeader(writeable: WireProtocolWriteable, idMapping: Array[Int],
                          i: Int, length: Int): Unit = {
    writeable.writeValueHeader(valueId(i, idMapping), tableSchema.getColumnType(i), false, length)
  }

  private def writeBytes(writeable: WireProtocolWriteable, idMapping: Array[Int],
                         i: Int, bytes: Array[Byte]): Unit = {
    writeHeader(writeable, idMapping, i, bytes.length)
    writeable.onBytes(bytes)
  }

  @tailrec
  final def writeRowsInternal(writer: TableWriter[InternalRow],
                              rows: java.util.ArrayList[InternalRow],
                              timeout: Duration): Unit = {
    if (!writer.write(rows, tableSchema)) {
      log.debugLazy("Waiting for writer ready event")
      YtMetricsRegister.time(writeReadyEventTime, writeReadyEventTimeSum) {
        writer.readyEvent().get(timeout.toMillis, TimeUnit.MILLISECONDS)
      }
      writeRowsInternal(writer, rows, timeout)
    }
  }

  override def serializeRow(row: InternalRow,
                            writeable: WireProtocolWriteable,
                            keyFieldsOnly: Boolean,
                            idMapping: Array[Int]): Unit = {
    writeable.writeValueCount(row.numFields)
    for {
      i <- 0 until row.numFields
    } {
      if (row.isNullAt(i)) {
        writeable.writeValueHeader(valueId(i, idMapping), ColumnValueType.NULL, false, 0)
      } else {
        schema(i).dataType match {
          case BinaryType | YsonType => writeBytes(writeable, idMapping, i, row.getBinary(i))
          case StringType => writeBytes(writeable, idMapping, i, row.getUTF8String(i).getBytes)
          case t@(ArrayType(_, _) | StructType(_) | MapType(_, _, _)) =>
            val skipNulls = schema(i).metadata.contains("skipNulls") && schema(i).metadata.getBoolean("skipNulls")
            writeBytes(writeable, idMapping, i, YsonEncoder.encode(row.get(i, schema(i).dataType), t, skipNulls))
          case atomic =>
            writeHeader(writeable, idMapping, i, 0)
            atomic match {
              case ByteType => writeable.onInteger(row.getByte(i))
              case ShortType => writeable.onInteger(row.getShort(i))
              case IntegerType => writeable.onInteger(row.getInt(i))
              case LongType => writeable.onInteger(row.getLong(i))
              case BooleanType => writeable.onBoolean(row.getBoolean(i))
              case FloatType => writeable.onDouble(row.getFloat(i))
              case DoubleType => writeable.onDouble(row.getDouble(i))
            }
        }
      }
    }
  }
}

object InternalRowSerializer {
  private val deserializers: ThreadLocal[mutable.Map[StructType, InternalRowSerializer]] = ThreadLocal.withInitial(() => mutable.ListMap.empty)
  private val context = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

  def getOrCreate(schema: StructType,
                  schemaHint: Map[String, YtLogicalType],
                  filters: Array[Filter] = Array.empty): InternalRowSerializer = {
    deserializers.get().getOrElseUpdate(schema, new InternalRowSerializer(schema, schemaHint))
  }

  final def writeRows(writer: TableWriter[InternalRow],
                      rows: java.util.ArrayList[InternalRow],
                      timeout: Duration): Future[Unit] = {
    Future {
      writeRowsRecursive(writer, rows, timeout)
    }(context)
  }

  @tailrec
  private def writeRowsRecursive(writer: TableWriter[InternalRow],
                                 rows: java.util.ArrayList[InternalRow],
                                 timeout: Duration): Unit = {
    if (!writer.write(rows)) {
      YtMetricsRegister.time(writeReadyEventTime, writeReadyEventTimeSum) {
        writer.readyEvent().get(timeout.toMillis, TimeUnit.MILLISECONDS)
      }
      writeRowsRecursive(writer, rows, timeout)
    }
  }
}


