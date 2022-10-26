package org.apache.spark.sql.v2

import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, MapType, Metadata, MetadataBuilder, NullType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks
import ru.yandex.spark.yt.SchemaTestUtils

class YtScanBuilderTest extends FlatSpec with Matchers with SchemaTestUtils with TableDrivenPropertyChecks {
  behavior of "YtScanBuilder"

  it should "push metadata in struct type" in {
    val requiredSchema = StructType(Seq(
      structField("a", DoubleType, nullable = false)))
    val dataSchema = StructType(Seq(
      structField("a", DoubleType, nullable = false, metadata = new MetadataBuilder().putString("a", "b")),
      structField("b", IntegerType)))
    val result = YtScanBuilder.pushStructMetadata(requiredSchema, dataSchema)
    result shouldBe StructType(Seq(
      structField("a", DoubleType, nullable = false, metadata = new MetadataBuilder().putString("a", "b"))
    ))
  }

  it should "push metadata in nested struct type" in {
    val requiredSchema = StructType(Seq(
      structField("b", StructType(Seq(
        StructField("c", BooleanType)
      )))
    ))
    val dataSchema = StructType(Seq(
      structField("a", DoubleType, nullable = false, metadata = new MetadataBuilder().putString("a", "b")),
      structField("b", StructType(Seq(
        StructField("c", BooleanType, nullable = false, metadata = new MetadataBuilder().putString("s", "t").build())
      )))
    ))
    val result = YtScanBuilder.pushStructMetadata(requiredSchema, dataSchema)
    result shouldBe StructType(Seq(
      structField("b", StructType(Seq(
        StructField("c", BooleanType, metadata = new MetadataBuilder().putString("s", "t").build())
      )))
    ))
  }

  it should "push metadata in nested composite types" in {
    val requiredSchema = StructType(Seq(
      structField("b", ArrayType(MapType(StructType(Seq(
        StructField("p", NullType, nullable = false)
      )), StringType), containsNull = false))))
    val dataSchema = StructType(Seq(
      structField("b", ArrayType(MapType(StructType(Seq(
        StructField("p", NullType, nullable = false, metadata = new MetadataBuilder().putString("0", "A").putString("1", "B").build())
      )), StringType), containsNull = false), nullable = false, metadata = new MetadataBuilder().putString("2", "C"))
    ))
    val result = YtScanBuilder.pushStructMetadata(requiredSchema, dataSchema)
    result shouldBe StructType(Seq(
      structField("b", ArrayType(MapType(StructType(Seq(
        StructField("p", NullType, nullable = false, metadata = new MetadataBuilder().putString("0", "A").putString("1", "B").build())
      )), StringType), containsNull = false), metadata = new MetadataBuilder().putString("2", "C"))
    ))
  }
}
