package ru.yandex.spark.yt.test

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import ru.yandex.inside.yt.kosher.impl.ytree.YTreeNodeUtils
import ru.yandex.inside.yt.kosher.impl.ytree.`object`.YTreeSerializer
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.{YTreeConsumer, YTreeTextSerializer}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.misc.reflection.ClassX
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.table.OptimizeMode
import ru.yandex.yt.ytclient.`object`.{WireRowDeserializer, WireValueDeserializer}
import ru.yandex.yt.ytclient.proxy.YtClient
import ru.yandex.yt.ytclient.proxy.request.{ObjectType, WriteTable}
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps

trait TestUtils {
  def createEmptyTable(path: String, schema: TableSchema)
                      (implicit yt: YtClient): Unit = {
    import scala.collection.JavaConverters._
    yt.createNode(path, ObjectType.Table, Map("schema" -> schema.toYTree).asJava).join()
  }

  def readTableAsYson(path: String, schema: TableSchema)(implicit yt: YtClient): Seq[YTreeNode] = {
    val deser = new WireRowDeserializer[YTreeNode] with WireValueDeserializer[Unit] {
      private var builder = new YTreeBuilder().beginMap()

      override def onNewRow(i: Int): WireValueDeserializer[_] = {
        builder = new YTreeBuilder().beginMap()
        this
      }

      override def onCompleteRow(): YTreeNode = builder.endMap().build()

      override def setId(i: Int): Unit = builder.key(schema.getColumnName(i))

      override def setType(columnValueType: ColumnValueType): Unit = {}

      override def setAggregate(b: Boolean): Unit = {}

      override def setTimestamp(l: Long): Unit = {}

      override def build(): Unit = {}

      override def onEntity(): Unit = builder.entity()

      override def onInteger(value: Long): Unit = builder.value(value)

      override def onBoolean(value: Boolean): Unit = builder.value(value)

      override def onDouble(value: Double): Unit = builder.value(value)

      override def onBytes(bytes: Array[Byte]): Unit = builder.value(bytes)
    }
    YtWrapper.readTable(path, deser, 1 minute).toList
  }

  def writeTableFromYson(rows: Seq[String], path: String, schema: TableSchema,
                         optimizeFor: OptimizeMode = OptimizeMode.Scan,
                         options: Map[String, YTreeNode] = Map.empty)
                        (implicit yt: YtClient): Unit = {
    writeTableFromYson(rows, path, schema.toYTree, schema, optimizeFor, options)
  }

  def writeTableFromYson(rows: Seq[String], path: String, schema: YTreeNode, physicalSchema: TableSchema,
                         optimizeFor: OptimizeMode, options: Map[String, YTreeNode])
                        (implicit yt: YtClient): Unit = {
    import scala.collection.JavaConverters._

    val serializer = new YTreeSerializer[String] {
      override def serialize(obj: String, consumer: YTreeConsumer): Unit = {
        val node = YTreeTextSerializer.deserialize(new ByteArrayInputStream(obj.getBytes(StandardCharsets.UTF_8)))
        YTreeNodeUtils.walk(node, consumer, false)
      }

      override def getClazz: ClassX[String] = ClassX.wrap(classOf[String])

      override def deserialize(node: YTreeNode): String = ???
    }
    YtWrapper.createTable(path, options ++ Map("schema" -> schema, "optimize_for" -> optimizeFor.node), None)
    val writer = yt.writeTable(new WriteTable[String](path, serializer)).join()

    @tailrec
    def write(): Unit = {
      if (!writer.write(rows.asJava, physicalSchema)) {
        writer.readyEvent().join()
        write()
      }
    }

    write()
    writer.close().join()
  }

}
