package org.apache.spark.sql.v2

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkException
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtUtils.{FileWithSchema, _}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.fs.path.YPathEnriched.YtRootPath
import tech.ytsaurus.spyt.fs.{YtFileStatus, YtHadoopPath, YtTableMeta}
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode
import tech.ytsaurus.spyt.SchemaTestUtils

import scala.util.Try

class YtUtilsTest extends FlatSpec with Matchers with SchemaTestUtils with TableDrivenPropertyChecks {

  behavior of "YtUtilsTest"

  it should "mergeFileSchemas" in {
    val a = structField("a", StringType, metadata = new MetadataBuilder().putString("custom", "my metadata"))
    val b = structField("b", LongType)
    val c = structField("c", DoubleType, originalName = Some("x"))

    val ss = Seq(
      FileWithSchema(fileStatus("/path0"), StructType(Seq(a))),
      FileWithSchema(fileStatus("/path1"), StructType(Seq(a))),
      FileWithSchema(fileStatus("/path2"), StructType(Seq(a, b))),
      FileWithSchema(fileStatus("/path3"), StructType(Seq(a, b))),
      FileWithSchema(fileStatus("/path4"), StructType(Seq(a, b, c))),
      FileWithSchema(fileStatus("/path5"), StructType(Seq(c, b, a))),
      FileWithSchema(fileStatus("/path6"), StructType(Seq(a.withKeyId(0), b, c))),
      FileWithSchema(fileStatus("/path7"), StructType(Seq(a.withKeyId(0), b, c))),
      FileWithSchema(fileStatus("/path8"), StructType(Seq(a, b, c))),
      FileWithSchema(fileStatus("/path9"), StructType(Seq(a, b.setNullable(false), c))),
      FileWithSchema(fileStatus("/path10"), StructType(Seq(c))),
      FileWithSchema(fileStatus("/path11"), StructType(Seq(a.copy(dataType = LongType)))),
    )

    val table = Table(
      ("schemas", "enableMerge", "expected"),

      (Seq(), false, Right(None)),
      (ss.slice(0, 1), false, Right(Some(ss.head.schema))),
      (ss.slice(0, 2), false, Right(Some(ss.head.schema))),
      (ss.slice(0, 3), false, Left(classOf[SparkException])),
      (ss.slice(0, 4), false, Left(classOf[SparkException])),
      (ss.slice(2, 4), false, Right(Some(ss(2).schema))),
      (ss.slice(2, 5), false, Left(classOf[SparkException])),
      (ss.slice(4, 6), false, Right(Some(ss(4).schema))),
      (ss.slice(4, 7), false, Right(Some(ss(4).schema))), // ignore key id
      (ss.slice(4, 9), false, Right(Some(ss(4).schema))), // ignore key id
      (ss.slice(6, 9), false, Right(Some(ss(4).schema))), // ignore key id
      (ss.slice(6, 8), false, Right(Some(ss(6).schema))),
      (ss.slice(4, 6) :+ ss(9), false, Left(classOf[SparkException])),
      (ss.slice(0, 2) :+ ss(10), false, Left(classOf[SparkException])),
      (ss.slice(0, 2) :+ ss(11), false, Left(classOf[SparkException])),


      (ss.slice(0, 1), true, Right(Some(ss.head.schema))),
      (ss.slice(0, 2), true, Right(Some(ss.head.schema))),
      (ss.slice(0, 3), true, Right(Some(ss(2).schema))),
      (ss.slice(0, 4), true, Right(Some(ss(2).schema))),
      (ss.slice(2, 4), true, Right(Some(ss(2).schema))),
      (ss.slice(2, 5), true, Right(Some(ss(4).schema))),
      (ss.slice(4, 6), true, Right(Some(ss(4).schema))),
      (ss.slice(4, 7), true, Right(Some(ss(4).schema))), // ignore key id
      (ss.slice(4, 9), true, Right(Some(ss(4).schema))), // ignore key id
      (ss.slice(6, 9), true, Right(Some(ss(4).schema))), // ignore key id
      (ss.slice(6, 8), true, Right(Some(ss(6).schema))),
      (ss.slice(4, 6) :+ ss(9), true, Right(Some(ss(4).schema))),
      (ss.slice(0, 2) :+ ss(10), true, Right(Some(StructType(Seq(a, c))))),
      (ss.slice(0, 2) :+ ss(11), true, Left(classOf[SparkException]))
    )

    forAll(table){
      (schemas: Seq[FileWithSchema], enableMerge: Boolean, expected: Either[Class[_], Option[StructType]]) =>
        val res = Try(YtUtils.mergeFileSchemas(schemas, enableMerge))
        expected match {
          case Right(schema) => res.get shouldEqual schema
          case Left(clazz) => res.failed.get.getClass shouldEqual clazz
        }
    }
  }

  it should "checkAllEquals" in {
    val a = structField("a", StringType, metadata = new MetadataBuilder().putString("custom", "my metadata"))
    val b = structField("b", LongType)
    val c = structField("c", DoubleType, originalName = Some("x"))

    val ss = Seq(
      FileWithSchema(fileStatus("/path0"), StructType(Seq(a))),
      FileWithSchema(fileStatus("/path1"), StructType(Seq(a))),
      FileWithSchema(fileStatus("/path2"), StructType(Seq(a, b))),
      FileWithSchema(fileStatus("/path3"), StructType(Seq(a, b))),
      FileWithSchema(fileStatus("/path4"), StructType(Seq(a, b, c))),
      FileWithSchema(fileStatus("/path5"), StructType(Seq(c, b, a))),
      FileWithSchema(fileStatus("/path6"), StructType(Seq(a.withKeyId(0), b, c))),
      FileWithSchema(fileStatus("/path7"), StructType(Seq(a, b.setNullable(false), c)))
    )

    val table = Table(
      ("schemas", "expected"),
      (Seq(), Right(None)),
      (ss.slice(0, 1), Right(Some(ss.head.schema))),
      (ss.slice(0, 2), Right(Some(ss.head.schema))),
      (ss.slice(0, 3), Left(SchemaDiscrepancy(ss.head, ss(2)))),
      (ss.slice(0, 4), Left(SchemaDiscrepancy(ss.head, ss(2)))),
      (ss.slice(2, 4), Right(Some(ss(2).schema))),
      (ss.slice(2, 5), Left(SchemaDiscrepancy(ss(2), ss(4)))),
      (ss.slice(4, 6), Right(Some(ss(4).schema))),
      (ss.slice(4, 7), Left(SchemaDiscrepancy(ss(4), ss(6)))),
      (ss.slice(4, 6) :+ ss(7), Left(SchemaDiscrepancy(ss(4), ss(7))))
    )

    forAll(table){ (schemas: Seq[FileWithSchema], expected: Either[SchemaDiscrepancy, Option[StructType]]) =>
      YtUtils.checkAllEquals(schemas) shouldEqual expected
    }
  }

  it should "dropKeyFieldsMetadata from schema" in {
    val schema = StructType(Seq(
      structField("a", StringType, keyId = 0, metadata = new MetadataBuilder().putString("custom", "my metadata")),
      structField("b", LongType, keyId = 1),
      structField("c", DoubleType, originalName = Some("x"), keyId = 2),
      structField("d", StringType, metadata = new MetadataBuilder().putString("custom 2", "my metadata 2")),
      structField("e", LongType),
      structField("f", DoubleType, originalName = Some("x"))
    ))

    val res = YtUtils.dropKeyFieldsMetadata(schema)

    res should contain theSameElementsAs Seq(
      structField("a", StringType, metadata = new MetadataBuilder().putString("custom", "my metadata")),
      structField("b", LongType),
      structField("c", DoubleType, originalName = Some("x")),
      structField("d", StringType, metadata = new MetadataBuilder().putString("custom 2", "my metadata 2")),
      structField("e", LongType),
      structField("f", DoubleType, originalName = Some("x"))
    )
  }

  private def fileStatus(path: String): FileStatus = {
    val ytPath = YtRootPath(new Path(path))
    val chunkPath = YtHadoopPath(ytPath, YtTableMeta(1, 1, 0, OptimizeMode.Scan, isDynamic = false))
    YtFileStatus.toFileStatus(chunkPath)
  }

}
