package tech.ytsaurus.spyt.format

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, LongType, Metadata, MetadataBuilder, StringType, StructField, StructType}
import org.apache.spark.sql.v2.YtUtils
import org.mockito.Mockito
import org.mockito.scalatest.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.serializers.SchemaConverter.MetadataFields
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.GetNode
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.SchemaTestUtils
import tech.ytsaurus.spyt.fs.YtTableFileSystem
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

class YtInferSchemaTest extends FlatSpec with Matchers with LocalSpark
  with TmpDir with SchemaTestUtils with MockitoSugar with TestUtils {
  behavior of "YtDataSource"

  import spark.implicits._

  private val atomicSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("a", ColumnValueType.INT64)
    .addValue("b", ColumnValueType.STRING)
    .addValue("c", ColumnValueType.DOUBLE)
    .build()

  it should "accept schema hint for case-sensitive column name" in {
    Seq(
      Seq(1, 2, 3),
      Seq(4, 5, 6)
    ).toDF("EventValue").write.yt(tmpPath)

    val res = spark.read
      .schemaHint("EventValue" -> ArrayType(IntegerType))
      .yt(tmpPath)
      .schema
      .find(_.name == "EventValue")
      .get

    res.dataType shouldEqual ArrayType(IntegerType)
  }

  it should "read dataset with column name containing spaces and digits" in {
    val columnName = "3a b"
    writeTableFromYson(Seq(
      s"""{"$columnName" = 1}""",
      s"""{"$columnName" = 2}"""
    ), tmpPath, TableSchema.builder().addKey(columnName, ColumnValueType.INT64).build())

    val res = spark.read.yt(tmpPath)

    res.columns should contain theSameElementsAs Seq(columnName)
    res.select(columnName).collect() should contain theSameElementsAs Seq(
      Row(1),
      Row(2)
    )
  }

  it should "read dataset with column name containing dots" in {
    val columnName = "a.b"
    val sparkColumnName = columnName.replace('.', '_')
    writeTableFromYson(Seq(
      s"""{"$columnName" = 1}""",
      s"""{"$columnName" = 2}"""
    ), tmpPath, TableSchema.builder().addKey(columnName, ColumnValueType.INT64).build())

    val res = spark.read.yt(tmpPath)

    res.columns should contain theSameElementsAs Seq(sparkColumnName)
    res.select(sparkColumnName).collect() should contain theSameElementsAs Seq(
      Row(1),
      Row(2)
    )
  }

  it should "read tables with different schemas when mergeSchema is enabled in read's option" in {
    YtWrapper.createDir(tmpPath)
    val table1 = s"$tmpPath/t1"
    val table2 = s"$tmpPath/t2"
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}"""
    ), table1, atomicSchema)
    writeTableFromYson(Seq(
      """{c = 2.0; d = "t"}"""
    ), table2, TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("c", ColumnValueType.DOUBLE)
      .addValue("d", ColumnValueType.STRING)
      .build()
    )

    val df = spark.read.option("mergeSchema", "true").yt(table1, table2)

    df.columns should contain theSameElementsAs Seq("a", "b", "c", "d")
    df.select("a", "b", "c", "d").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3, null),
      Row(null, null, 2.0, "t")
    )
  }

  it should "read tables with different schemas when mergeSchema is enabled in spark conf" in {
    YtWrapper.createDir(tmpPath)
    val table1 = s"$tmpPath/t1"
    val table2 = s"$tmpPath/t2"
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}"""
    ), table1, atomicSchema)
    writeTableFromYson(Seq(
      """{c = 2.0; d = "t"}"""
    ), table2, TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("c", ColumnValueType.DOUBLE)
      .addValue("d", ColumnValueType.STRING)
      .build()
    )
    val df = withConf("spark.sql.yt.mergeSchema", "true") {
      spark.read.yt(table1, table2)
    }

    df.columns should contain theSameElementsAs Seq("a", "b", "c", "d")
    df.select("a", "b", "c", "d").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3, null),
      Row(null, null, 2.0, "t")
    )
  }

  it should "read tables with same schemas but different order" in {
    YtWrapper.createDir(tmpPath)
    val table1 = s"$tmpPath/t1"
    val table2 = s"$tmpPath/t2"
    writeTableFromYson(Seq(
      """{a = 1; b = "f"; c = 0.3}"""
    ), table1, atomicSchema)
    writeTableFromYson(Seq(
      """{c = 1.2; a = 2; b = "g"}"""
    ), table2, TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("c", ColumnValueType.DOUBLE)
      .addValue("a", ColumnValueType.INT64)
      .addValue("b", ColumnValueType.STRING)
      .build()
    )
    val df = spark.read.yt(table1, table2)

    df.columns should contain theSameElementsAs Seq("a", "b", "c")
    df.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, "f", 0.3),
      Row(2, "g", 1.2)
    )
  }

  it should "fail reading tables with incompatible schemas" in {
    YtWrapper.createDir(tmpPath)
    val table1 = s"$tmpPath/t1"
    val table2 = s"$tmpPath/t2"
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}"""
    ), table1, atomicSchema)
    writeTableFromYson(Seq(
      """{c = "a"}"""
    ), table2, TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("c", ColumnValueType.STRING)
      .build()
    )

    a[SparkException] shouldBe thrownBy {
      spark.read.option("mergeSchema", "true").yt(table1, table2)
    }
  }

  it should "fail reading tables with different schemas when mergeSchema is disabled" in {
    YtWrapper.createDir(tmpPath)
    val table1 = s"$tmpPath/t1"
    val table2 = s"$tmpPath/t2"
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}"""
    ), table1, atomicSchema)
    writeTableFromYson(Seq(
      """{c = 0.2}"""
    ), table2, TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("c", ColumnValueType.DOUBLE)
      .build()
    )

    a[SparkException] shouldBe thrownBy {
      spark.read.yt(table1, table2)
    }
  }

  it should "generate nullable correct type_v1 schema" in {
    def createMetadata(name: String, keyId: Long = -1): Metadata = {
      new MetadataBuilder()
        .putLong(MetadataFields.KEY_ID, keyId)
        .putString(MetadataFields.ORIGINAL_NAME, name)
        .putBoolean(MetadataFields.ARROW_SUPPORTED, true)
        .build()
    }

    val data = Seq(
      (1L, Some(true), 1.0, Some("1"), Array[Byte](56, 52)),
      (3L, Some(false), 2.0, None, Array[Byte](56, 49))
    )
    withConf("spark.yt.schema.forcingNullableIfNoMetadata.enabled", "false") {
      data
        .toDF("a", "b", "c", "d", "e").coalesce(1)
        .write.yt(tmpPath)

      val res = spark.read.yt(tmpPath)

      res.schema shouldBe StructType(Seq(
        StructField("a", LongType, nullable = true, metadata = createMetadata("a")),
        StructField("b", BooleanType, nullable = true, metadata = createMetadata("b")),
        StructField("c", DoubleType, nullable = true, metadata = createMetadata("c")),
        StructField("d", StringType, nullable = true, metadata = createMetadata("d")),
        StructField("e", StringType, nullable = true, metadata = createMetadata("e"))))
    }
  }

  it should "infer table's schema one time" in {
    YtWrapper.createDir(tmpPath)
    val filesCount = 3
    val tables = (1 to filesCount).map(x => s"$tmpPath/t$x")
    tables.foreach {
      table => List(1, 2, 3, 4, 5).toDF().repartition(2).write.yt(table)
    }

    val fs = new YtTableFileSystem
    fs.initialize(new Path("/").toUri, spark.sparkContext.hadoopConfiguration)
    fs.setConf(spark.sparkContext.hadoopConfiguration)

    val filesStatus = tables.flatMap(x => fs.listStatus(new Path(s"ytTable:/$x")))

    val mockYt: CompoundClient = Mockito.spy(YtClientProvider.ytClient(ytClientConfiguration(spark)))
    YtUtils.inferSchema(spark, Map.empty, filesStatus)(mockYt)

    // getNode invoked in YtWrapper.attribute(path, "schema"), that might be invoked for every chunk in inferSchema
    // schema should be asked exactly 1 time for every file
    verify(mockYt, times(tables.length)).getNode(any[GetNode])
    Mockito.reset(mockYt)
  }

}
