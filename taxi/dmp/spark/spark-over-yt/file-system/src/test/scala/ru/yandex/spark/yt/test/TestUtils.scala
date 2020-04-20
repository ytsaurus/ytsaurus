package ru.yandex.spark.yt.test

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import ru.yandex.inside.yt.kosher.impl.ytree.YTreeNodeUtils
import ru.yandex.inside.yt.kosher.impl.ytree.`object`.YTreeSerializer
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.{YTreeConsumer, YTreeTextSerializer}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.misc.reflection.ClassX
import ru.yandex.yt.ytclient.proxy.YtClient
import ru.yandex.yt.ytclient.proxy.request.{ObjectType, WriteTable}
import ru.yandex.yt.ytclient.tables.TableSchema

import scala.annotation.tailrec

trait TestUtils {
  def createEmptyTable(path: String, schema: TableSchema)(implicit yt: YtClient): Unit = {
    import scala.collection.JavaConverters._
    yt.createNode(path, ObjectType.Table, Map("schema" -> schema.toYTree).asJava).join()
  }

  def writeTableFromYson(rows: Seq[String], path: String, schema: TableSchema)(implicit yt: YtClient): Unit = {
    writeTableFromYson(rows, path, schema.toYTree, schema)
  }

  def writeTableFromYson(rows: Seq[String], path: String, schema: YTreeNode, physicalSchema: TableSchema)(implicit yt: YtClient): Unit = {
    import scala.collection.JavaConverters._

    val serializer = new YTreeSerializer[String] {
      override def serialize(obj: String, consumer: YTreeConsumer): Unit = {
        val node = YTreeTextSerializer.deserialize(new ByteArrayInputStream(obj.getBytes(StandardCharsets.UTF_8)))
        YTreeNodeUtils.walk(node, consumer, false)
      }

      override def getClazz: ClassX[String] = ClassX.wrap(classOf[String])

      override def deserialize(node: YTreeNode): String = ???
    }
    yt.createNode(path, ObjectType.Table, Map("schema" -> schema).asJava).join()
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
