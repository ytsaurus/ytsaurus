package ru.yandex.spark.yt.test

import ru.yandex.inside.yt.kosher.cypress.YPath
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeNodeUtils
import ru.yandex.inside.yt.kosher.impl.ytree.`object`.YTreeSerializer
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.misc.reflection.ClassX
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.table.OptimizeMode
import ru.yandex.yson.YsonConsumer
import ru.yandex.yt.ytclient.`object`.{UnversionedRowSerializer, WireRowDeserializer, WireValueDeserializer}
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.proxy.request.{ObjectType, WriteTable}
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}
import ru.yandex.yt.ytclient.wire.UnversionedRow

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps

trait TestUtils {
  val longColumnSchema: TableSchema = new TableSchema.Builder()
    .setUniqueKeys(false)
    .addValue("value", ColumnValueType.INT64)
    .build()

  def createEmptyTable(path: String, schema: TableSchema)
                      (implicit yt: CompoundClient): Unit = {
    import scala.collection.JavaConverters._
    yt.createNode(path, ObjectType.Table, Map("schema" -> schema.toYTree).asJava).join()
  }

  def readTableAsYson(path: String, transaction: Option[String] = None)
                     (implicit yt: CompoundClient): Seq[YTreeNode] = {
    readTableAsYson(YPath.simple(YtWrapper.formatPath(path)), transaction)
  }

  def readTableAsYson(path: YPath, transaction: Option[String])
                     (implicit yt: CompoundClient): Seq[YTreeNode] = {
    val schema = TableSchema.fromYTree(YtWrapper.attribute(path, "schema", transaction))
    val deser = new WireRowDeserializer[YTreeNode] with WireValueDeserializer[Unit] {
      private var builder = new YTreeBuilder().beginMap()

      override def onNewRow(i: Int): WireValueDeserializer[_] = {
        builder = new YTreeBuilder().beginMap()
        this
      }

      override def onCompleteRow(): YTreeNode = builder.endMap().build()

      override def onNullRow(): YTreeNode = ???

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
    YtWrapper.readTable(path, deser, 1 minute, transaction, () => _).toList
  }

  def writeTableFromYson(rows: Seq[String], path: String, schema: TableSchema,
                         optimizeFor: OptimizeMode = OptimizeMode.Scan,
                         options: Map[String, YTreeNode] = Map.empty)
                        (implicit yt: CompoundClient): Unit = {
    writeTableFromYson(rows, path, schema.toYTree, schema, optimizeFor, options)
  }

  def writeTableFromYson(rows: Seq[String], path: String, schema: YTreeNode, physicalSchema: TableSchema,
                         optimizeFor: OptimizeMode, options: Map[String, YTreeNode])
                        (implicit yt: CompoundClient): Unit = {
    import scala.collection.JavaConverters._

    val serializer = new YTreeSerializer[String] {
      override def serialize(obj: String, consumer: YsonConsumer): Unit = {
        val node = YTreeTextSerializer.deserialize(new ByteArrayInputStream(obj.getBytes(StandardCharsets.UTF_8)))
        YTreeNodeUtils.walk(node, consumer, false)
      }

      override def getClazz: ClassX[String] = ClassX.wrap(classOf[String])

      override def deserialize(node: YTreeNode): String = ???

      override def getColumnValueType: ColumnValueType = ColumnValueType.STRING
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

  def writeTableFromURow(rows: Seq[UnversionedRow], path: String,
                         physicalSchema: TableSchema, optimizeFor: OptimizeMode = OptimizeMode.Scan,
                         options: Map[String, YTreeNode] = Map.empty)
                        (implicit yt: CompoundClient): Unit = {
    import scala.collection.JavaConverters._

    YtWrapper.createTable(path, options ++ Map("schema" -> physicalSchema.toYTree,
      "optimize_for" -> optimizeFor.node), None)

    val writer = yt.writeTable(new WriteTable[UnversionedRow](path,
      new UnversionedRowSerializer(physicalSchema))).join()

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

  def writeFileFromStream(input: InputStream, path: String)
                         (implicit yt: CompoundClient): Unit = {
    YtWrapper.createFile(path)
    val out = YtWrapper.writeFile(path, 1 minute, None)
    try {
      val b = new Array[Byte](65536)
      Stream.continually(input.read(b)).takeWhile(_ > 0).foreach(out.write(b, 0, _))
    } finally {
      out.close()
    }
  }

  def writeFileFromString(input: String, path: String)
                         (implicit yt: CompoundClient): Unit = {
    writeFileFromStream(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), path)
  }


  def writeFileFromResource(inputPath: String, path: String)
                           (implicit yt: CompoundClient): Unit = {
    val in = getClass.getResourceAsStream(inputPath)
    try {
      writeFileFromStream(in, path)
    } finally {
      in.close()
    }
  }

  def writeComplexTable(path: String)(implicit yt: CompoundClient): Unit = {
    val ytSchema = new TableSchema.Builder()
      .setUniqueKeys(false)
      .addValue("f1", ColumnValueType.ANY)
      .addValue("f2", ColumnValueType.ANY)
      .addValue("f3", ColumnValueType.ANY)
      .addValue("f4", ColumnValueType.ANY)
      .addValue("f5", ColumnValueType.ANY)
      .addValue("f6", ColumnValueType.ANY)
      .addValue("f7", ColumnValueType.ANY)
      .addValue("f8", ColumnValueType.ANY)
      .addValue("f9", ColumnValueType.ANY)
      .addValue("f10", ColumnValueType.ANY)
      .addValue("f11", ColumnValueType.ANY)
      .build()
    writeTableFromYson(Seq(
      """{
        |f1={a={aa=1};b=#;c={cc=#}};
        |f2={a={a="aa"};b=#;c={a=#}};
        |f3={a=[0.1];b=#;c=[#]};
        |f4={a=%true;b=#};
        |f5={a={a=1;b=#};b={a="aa"};c=[%true;#];d=0.1};
        |f6=[{a=1;b=#};#];
        |f7=[{a="aa"};{a=#};#];
        |f8=[[1;#];#];
        |f9=[0.1;#];
        |f10=[[1;{a=%true}];[2;{b=%false}];[3;{c=#}];[4;#]];
        |f11=[[{a=%true};1];[{b=%false};2];[{c=#};3];];
        |}""".stripMargin
    ), path, ytSchema)
  }

}
