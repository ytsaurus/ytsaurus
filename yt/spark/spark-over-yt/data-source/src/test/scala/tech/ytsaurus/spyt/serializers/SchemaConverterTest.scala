package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtUtils
import org.apache.spark.sql.yson.{UInt64Type, YsonType}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.TypeV3
import SchemaConverter.{MetadataFields, Unordered, ytLogicalSchema}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.YtReader
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.SchemaTestUtils
import tech.ytsaurus.typeinfo.StructType.Member
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.{YTree, YTreeMapNode}

class SchemaConverterTest extends FlatSpec with Matchers
  with TestUtils with TmpDir with LocalSpark with SchemaTestUtils with MockitoSugar with TableDrivenPropertyChecks {
  behavior of "SchemaConverter"

  it should "convert yt schema to spark one" in {
    val schema = TableSchema.builder()
      .setUniqueKeys(false)
      .addKey("a", ColumnValueType.STRING)
      .addKey("c.e", ColumnValueType.INT64)
      .addValue("d", ColumnValueType.INT64)
      .build()
    val res = SchemaConverter.sparkSchema(schema.toYTree)
    res shouldBe StructType(Seq(
      structField("a", StringType, keyId = 0, nullable = true),
      structField("c_e", LongType, originalName = Some("c.e"), 1, nullable = true),
      structField("d", LongType, nullable = true)
    ))
  }

  private val schema = TableSchema.builder().setUniqueKeys(false)
    .addValue("NULL", ColumnValueType.NULL)
    .addValue("INT64", ColumnValueType.INT64)
    .addValue("int64_3", TiType.int64())
    .addValue("UINT64", ColumnValueType.UINT64)
    .addValue("uint64_3", TiType.uint64())
    .addValue("floatType", TiType.floatType())
    .addValue("DOUBLE", ColumnValueType.DOUBLE)
    .addValue("doubleType", TiType.doubleType())
    .addValue("BOOLEAN", ColumnValueType.BOOLEAN)
    .addValue("bool", TiType.bool())
    .addValue("STRING", ColumnValueType.STRING)
    .addValue("string_3", TiType.string())
    .addValue("ANY", ColumnValueType.ANY)
    .addValue("yson", TiType.optional(TiType.yson()))
    .addValue("int8", TiType.int8())
    .addValue("uint8", TiType.uint8())
    .addValue("int16", TiType.int16())
    .addValue("uint16", TiType.uint16())
    .addValue("int32", TiType.int32())
    .addValue("uint32", TiType.uint32())
    .addValue("utf8", TiType.utf8())
    .addValue("date", TiType.date())
    .addValue("datetime", TiType.datetime())
    .addValue("timestamp", TiType.timestamp())
    .addValue("interval", TiType.interval())
    .addValue("list", TiType.list(TiType.bool()))
    .addValue("dict", TiType.dict(TiType.doubleType(), TiType.string()))
    .addValue("struct", TiType.struct(new Member("a", TiType.string()), new Member("b", TiType.optional(TiType.yson()))))
    .addValue("tuple", TiType.tuple(TiType.bool(), TiType.date()))
    .addValue("variantOverStruct",
      TiType.variantOverStruct(java.util.List.of[Member](new Member("c", TiType.uint16()), new Member("d", TiType.timestamp()))))
    .addValue("variantOverTuple", TiType.variantOverTuple(TiType.floatType(), TiType.interval()))
    .build()

  private val sparkSchema = StructType(Seq(
    structField("Null", NullType, nullable = false),
    structField("Long", LongType, nullable = false),
    structField("UInt64", UInt64Type, nullable = false),
    structField("Float", FloatType, nullable = false),
    structField("Double", DoubleType, nullable = false),
    structField("Boolean", BooleanType, nullable = false),
    structField("String", StringType, nullable = false),
    structField("Yson", YsonType, nullable = false),
    structField("Byte", ByteType, nullable = false),
    structField("Short", ShortType, nullable = false),
    structField("Integer", IntegerType, nullable = false),
    structField("Date", DateType, nullable = false),
    structField("Timestamp", TimestampType, nullable = false),
    structField("Array", ArrayType(BooleanType, containsNull = false), nullable = false),
    structField("Map", MapType(DoubleType, StringType, valueContainsNull = false), nullable = false),
    structField("Struct", StructType(Seq(StructField("a", StringType, nullable = false), StructField("b", YsonType, nullable = false))), nullable = false),
    structField("Tuple", StructType(Seq(StructField("_1", BooleanType, nullable = false), StructField("_2", DateType, nullable = false))), nullable = false),
    structField("VariantOverStruct", StructType(Seq(StructField("_vc", IntegerType, nullable = false), StructField("_vd", LongType, nullable = false))), nullable = false),
    structField("VariantOverTuple", StructType(Seq(StructField("_v_1", FloatType, nullable = false), StructField("_v_2", LongType, nullable = false))), nullable = false)
  ))

  it should "read schema without parsing type v3" in {
    // in sparkSchema.toYTree no type_v1 type names
    spark.conf.set(s"spark.yt.${TypeV3.name}", value = false)
    createEmptyTable(tmpPath, schema)
    val res = spark.read.yt(tmpPath).schema

    spark.conf.set(s"spark.yt.${TypeV3.name}", value = true)
    val res2 = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, "false").yt(tmpPath).schema

    res shouldBe res2
    res shouldBe StructType(Seq(
      structField("NULL", NullType, nullable = true),
      structField("INT64", LongType, nullable = true),
      structField("int64_3", LongType, nullable = true),
      structField("UINT64", UInt64Type, nullable = true),
      structField("uint64_3", UInt64Type, nullable = true),
      structField("floatType", FloatType, nullable = true, arrowSupported = false),
      structField("DOUBLE", DoubleType, nullable = true),
      structField("doubleType", DoubleType, nullable = true),
      structField("BOOLEAN", BooleanType, nullable = true),
      structField("bool", BooleanType, nullable = true),
      structField("STRING", StringType, nullable = true),
      structField("string_3", StringType, nullable = true),
      structField("ANY", YsonType, nullable = true),
      structField("yson", YsonType, nullable = true),
      structField("int8", ByteType, nullable = true),
      structField("uint8", ShortType, nullable = true),
      structField("int16", ShortType, nullable = true),
      structField("uint16", IntegerType, nullable = true),
      structField("int32", IntegerType, nullable = true),
      structField("uint32", LongType, nullable = true),
      structField("utf8", StringType, nullable = true),
      structField("date", DateType, nullable = true, arrowSupported = false),
      structField("datetime", TimestampType, nullable = true, arrowSupported = false),
      structField("timestamp", LongType, nullable = true, arrowSupported = false),
      structField("interval", LongType, nullable = true, arrowSupported = false),
      structField("list", YsonType, nullable = true),
      structField("dict", YsonType, nullable = true),
      structField("struct", YsonType, nullable = true),
      structField("tuple", YsonType, nullable = true),
      structField("variantOverStruct", YsonType, nullable = true),
      structField("variantOverTuple", YsonType, nullable = true)
    ))
  }

  it should "convert supported types with parsing type v3" in {
    val res = SchemaConverter.sparkSchema(schema.toYTree, parsingTypeV3 = true)
    res shouldBe StructType(Seq(
      structField("NULL", NullType, nullable = false),
      structField("INT64", LongType, nullable = true),
      structField("int64_3", LongType, nullable = false),
      structField("UINT64", UInt64Type, nullable = true),
      structField("uint64_3", UInt64Type, nullable = false),
      structField("floatType", FloatType, nullable = false, arrowSupported = false),
      structField("DOUBLE", DoubleType, nullable = true),
      structField("doubleType", DoubleType, nullable = false),
      structField("BOOLEAN", BooleanType, nullable = true),
      structField("bool", BooleanType, nullable = false),
      structField("STRING", StringType, nullable = true),
      structField("string_3", StringType, nullable = false),
      structField("ANY", YsonType, nullable = true),
      structField("yson", YsonType, nullable = true),
      structField("int8", ByteType, nullable = false),
      structField("uint8", ShortType, nullable = false),
      structField("int16", ShortType, nullable = false),
      structField("uint16", IntegerType, nullable = false),
      structField("int32", IntegerType, nullable = false),
      structField("uint32", LongType, nullable = false),
      structField("utf8", StringType, nullable = false),
      structField("date", DateType, nullable = false, arrowSupported = false),
      structField("datetime", TimestampType, nullable = false, arrowSupported = false),
      structField("timestamp", LongType, nullable = false, arrowSupported = false),
      structField("interval", LongType, nullable = false, arrowSupported = false),
      structField("list", ArrayType(BooleanType, containsNull = false), nullable = false),
      structField("dict", MapType(DoubleType, StringType, valueContainsNull = false), nullable = false),
      structField("struct", StructType(Seq(StructField("a", StringType, nullable = false), StructField("b", YsonType, nullable = true))), nullable = false),
      structField("tuple", StructType(Seq(StructField("_1", BooleanType, nullable = false), StructField("_2", DateType, nullable = false))), nullable = false),
      structField("variantOverStruct", StructType(Seq(StructField("_vc", IntegerType, metadata = new MetadataBuilder().putBoolean("optional", false).build()),
        StructField("_vd", LongType, metadata = new MetadataBuilder().putBoolean("optional", false).build()))), nullable = false),
      structField("variantOverTuple", StructType(Seq(StructField("_v_1", FloatType, metadata = new MetadataBuilder().putBoolean("optional", false).build()),
        StructField("_v_2", LongType, metadata = new MetadataBuilder().putBoolean("optional", false).build()))), nullable = false)
    ))
  }

  it should "use schema hint" in {
    val schema = TableSchema.builder()
      .setUniqueKeys(false)
      .addKey("a", ColumnValueType.STRING)
      .addValue("b", ColumnValueType.INT64)
      .build()
    val res = SchemaConverter.sparkSchema(schema.toYTree,
      Some(StructType(Seq(structField("a", UInt64Type, Some("x"), 1, nullable = false))))
    )
    res shouldBe StructType(Seq(
      structField("a", UInt64Type, keyId = 0, nullable = false),
      structField("b", LongType, nullable = true)
    ))
  }

  it should "convert spark schema to yt one with parsing type v3" in {
    val res = TableSchema.fromYTree(ytLogicalSchema(sparkSchema, Unordered, Map.empty, typeV3Format = true))
    res shouldBe TableSchema.builder().setUniqueKeys(false)
      .addValue("Null", TiType.nullType())
      .addValue("Long", TiType.int64())
      .addValue("UInt64", TiType.uint64())
      .addValue("Float", TiType.floatType())
      .addValue("Double", TiType.doubleType())
      .addValue("Boolean", TiType.bool())
      .addValue("String", TiType.string())
      .addValue("Yson", TiType.yson())
      .addValue("Byte", TiType.int8())
      .addValue("Short", TiType.int16())
      .addValue("Integer", TiType.int32())
      .addValue("Date", TiType.date())
      .addValue("Timestamp", TiType.datetime())
      .addValue("Array", TiType.list(TiType.bool()))
      .addValue("Map", TiType.dict(TiType.doubleType(), TiType.string()))
      .addValue("Struct", TiType.struct(new Member("a", TiType.string()), new Member("b", TiType.yson())))
      .addValue("Tuple", TiType.tuple(TiType.bool(), TiType.date()))
      .addValue("VariantOverStruct",
        TiType.variantOverStruct(java.util.List.of[Member](new Member("c", TiType.int32()), new Member("d", TiType.int64()))))
      .addValue("VariantOverTuple", TiType.variantOverTuple(TiType.floatType(), TiType.int64()))
      .build()
  }

  it should "convert spark schema to yt one" in {
    import scala.collection.JavaConverters._
    def getColumn(name: String, t: String): YTreeMapNode = {
      YTree.builder.beginMap.key("name").value(name).key("type").value(t)
        .key("required").value(false).buildMap
    }
    val res = ytLogicalSchema(sparkSchema, Unordered, Map.empty, typeV3Format = false)
    res shouldBe YTree.builder
      .beginAttributes
      .key("strict").value(true)
      .key("unique_keys").value(false)
      .endAttributes
      .value(Seq(
        getColumn("Null", "null"),
        getColumn("Long", "int64"),
        getColumn("UInt64", "uint64"),
        getColumn("Float", "float"),
        getColumn("Double", "double"),
        getColumn("Boolean", "boolean"),
        getColumn("String", "string"),
        getColumn("Yson", "any"),
        getColumn("Byte", "int8"),
        getColumn("Short", "int16"),
        getColumn("Integer", "int32"),
        getColumn("Date", "date"),
        getColumn("Timestamp", "datetime"),
        getColumn("Array", "any"),
        getColumn("Map", "any"),
        getColumn("Struct", "any"),
        getColumn("Tuple", "any"),
        getColumn("VariantOverStruct", "any"),
        getColumn("VariantOverTuple", "any")
      ).asJava)
      .build
  }

  it should "get keys from schema" in {
    def createMetadata(name: String, keyId: Long): Metadata = {
      new MetadataBuilder()
        .putLong(MetadataFields.KEY_ID, keyId)
        .putString(MetadataFields.ORIGINAL_NAME, name)
        .build()
    }

    val schema1 = StructType(Seq())
    SchemaConverter.keys(schema1) shouldBe Seq()

    val schema2 = StructType(Seq(
      StructField("a", LongType, metadata = createMetadata("a", 0))
    ))
    SchemaConverter.keys(schema2) shouldBe Seq(Some("a"))

    val schema3 = StructType(Seq(
      StructField("b", LongType, metadata = createMetadata("b", 1)),
      StructField("c", LongType, metadata = createMetadata("c", 2))
    ))
    SchemaConverter.keys(schema3) shouldBe Seq(None, Some("b"), Some("c"))

    val schema4 = StructType(Seq(
      StructField("c", LongType, metadata = createMetadata("c", 2)),
      StructField("a", LongType, metadata = createMetadata("a", 0))
    ))
    SchemaConverter.keys(schema4) shouldBe Seq(Some("a"), None, Some("c"))
  }
}
