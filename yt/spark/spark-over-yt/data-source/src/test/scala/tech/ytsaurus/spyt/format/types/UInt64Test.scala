package tech.ytsaurus.spyt.format.types

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.yson.UInt64Long.{fromStringUdf, toStringUdf}
import org.apache.spark.sql.yson.{UInt64Long, UInt64Type}
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.ysontree.YTree

class UInt64Test extends FlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {
  behavior of "YtDataSource"

  import spark.implicits._

  it should "read a table with uint64 column" in {
    val tableSchema = TableSchema.builder()
      .addValue("id", ColumnValueType.UINT64)
      .addValue("value", ColumnValueType.STRING)
      .build()

    writeTableFromYson(Seq(
      """{id = 1u; value = "value 1"}""",
      """{id = 2u; value = "value 2"}""",
      """{id = 3u; value = "value 3"}""",
      """{id = 9223372036854775816u; value = "value 4"}""",
      """{id = 9223372036854775813u; value = "value 5"}""",
      """{id = 18446744073709551615u; value = "value 6"}""" // 2*64 - 1
    ), tmpPath, tableSchema)

    val df = spark.read.yt(tmpPath)
    df.schema.fields.map(_.copy(metadata = Metadata.empty)) should contain theSameElementsInOrderAs Seq(
      StructField("id", UInt64Type),
      StructField("value", StringType)
    )
    df.select("id").as[UInt64Long].collect() should contain theSameElementsAs Seq(
      UInt64Long(1L), UInt64Long(2L), UInt64Long(3L), UInt64Long("9223372036854775813"),
      UInt64Long("9223372036854775816"), UInt64Long("18446744073709551615")
    )
  }

  it should "write to a table with uint64 column" in {
    val df = Seq(
      UInt64Long(1L), UInt64Long(2L), UInt64Long(3L), UInt64Long("9223372036854775813"),
      UInt64Long("9223372036854775816"), UInt64Long("18446744073709551615")
    ).toDF("a")
    df.write.yt(tmpPath)

    val schema = TableSchema.fromYTree(YtWrapper.attribute(tmpPath, "schema"))
    schema.getColumnNames should contain theSameElementsAs Seq("a")
    schema.getColumnNames should have size 1
    schema.getColumnType(0) shouldEqual ColumnValueType.UINT64

    val data = readTableAsYson(tmpPath).map(_.asMap().get("a").longValue())
    val expected = Seq(1L, 2L, 3L, java.lang.Long.parseUnsignedLong("9223372036854775813"),
      java.lang.Long.parseUnsignedLong("9223372036854775816"), java.lang.Long.parseUnsignedLong("18446744073709551615"))

    data should contain theSameElementsAs expected
  }

  it should "cast UInt64Long to Long" in {
    import org.apache.spark.sql.functions._
    val df = Seq(UInt64Long(1L), UInt64Long(2L), null,
      UInt64Long("9223372036854775816"), UInt64Long("18446744073709551615"))
      .toDF("a").withColumn("a", col("a").cast(LongType))

    df.select("a").collect() should contain theSameElementsAs Seq(
      Row(1L), Row(2L), Row(null), Row(-9223372036854775800L), Row(-1L)
    )
  }

  it should "cast Long to UInt64Long" in {
    import org.apache.spark.sql.functions._
    val df = Seq(Some(1L), Some(2L), None, Some(-1L))
      .toDF("a").withColumn("a", col("a").cast(UInt64Type))

    df.select("a").collect() should contain theSameElementsAs Seq(
      Row(UInt64Long(1L)), Row(UInt64Long(2L)), Row(null), Row(UInt64Long("18446744073709551615"))
    )
  }

  it should "cast UInt64Long to String" in {
    import org.apache.spark.sql.functions._
    val df = Seq(UInt64Long(1L), UInt64Long(2L), null,
      UInt64Long("9223372036854775816"), UInt64Long("18446744073709551615"))
      .toDF("a").withColumn("a", toStringUdf(col("a")))

    df.select("a").collect() should contain theSameElementsAs Seq(
      Row("1"), Row("2"), Row(null), Row("9223372036854775816"), Row("18446744073709551615")
    )
  }

  it should "cast String to UInt64Long" in {
    import org.apache.spark.sql.functions._
    val df = Seq("1", "2", null, "9223372036854775816", "18446744073709551615")
      .toDF("a").withColumn("a", fromStringUdf(col("a")))

    df.select("a").collect() should contain theSameElementsAs Seq(
      Row(UInt64Long(1L)), Row(UInt64Long(2L)), Row(null),
      Row(UInt64Long("9223372036854775816")), Row(UInt64Long("18446744073709551615"))
    )
  }

  it should "read UInt64Long" in {
    writeTableFromYson(Seq(
      """{a = 1}""",
      """{a = 2}""",
      """{a = #}"""
    ), tmpPath, TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("a", ColumnValueType.UINT64)
      .build()
    )

    val res = spark.read.yt(tmpPath)
    res.select("a").collect() should contain theSameElementsAs Seq(
      Row(UInt64Long(1L)), Row(UInt64Long(2L)), Row(null)
    )
  }

  it should "write UInt64Long" in {
    Seq(UInt64Long(1L), UInt64Long(2L), null)
      .toDF("a").write.yt(tmpPath)

    val res = spark.read.yt(tmpPath)
    res.select("a").collect() should contain theSameElementsAs Seq(
      Row(UInt64Long(1L)), Row(UInt64Long(2L)), Row(null)
    )
  }

  it should "group by uint64 column" in {
    Seq(UInt64Long(1L), UInt64Long(2L), UInt64Long(2L), UInt64Long(2L), UInt64Long(3L), UInt64Long(1L))
      .toDF("a").write.yt(tmpPath)

    val df = spark.read.yt(tmpPath)

    val res = df.groupBy("a").count().collect()
    res should contain theSameElementsAs Seq(
      Row(UInt64Long(1L), 2L),
      Row(UInt64Long(2L), 3L),
      Row(UInt64Long(3L), 1L)
    )
  }

  it should "support ObjectHashAggregation" in {
    val dataset = Seq(
      (UInt64Long(0), Seq.empty[UInt64Long]),
      (UInt64Long(1), Seq(UInt64Long(1), UInt64Long(1))),
      (UInt64Long(0), Seq(UInt64Long(2), UInt64Long(2)))).toDF("id", "nums")
    val q = dataset.groupBy("id").agg(collect_list("nums"))

    q.collect() should contain theSameElementsAs Seq(
      Row(UInt64Long(0), Seq(Seq.empty[UInt64Long], Seq(UInt64Long(2), UInt64Long(2)))),
      Row(UInt64Long(1), Seq(Seq(UInt64Long(1), UInt64Long(1))))
    )
  }

  it should "support fallback to sort for ObjectHashAggregation and compile without error" in {
    val dataset = (0 until 1000 map (i => (UInt64Long(i), 0 to i map(UInt64Long(_))))).toDF("id", "nums")
    val q = dataset.groupBy("id").agg(collect_list("nums"))
    q.collect()
  }

  it should "sort by uint64 column" in {
    val data = Seq(UInt64Long("9223372036854775813"),
      UInt64Long(0L), UInt64Long(1L), UInt64Long("9223372036854775816"))
    val sortedData = Seq(UInt64Long(0L), UInt64Long(1L), UInt64Long("9223372036854775813"),
      UInt64Long("9223372036854775816"))
    data.toDF("a").write.yt(tmpPath)

    val df = spark.read.yt(tmpPath)
    val res = df.sort("a").collect()
    res shouldBe sortedData.map(Row(_))
  }

  it should "write and read big uint64" in {
    val df = Seq(UInt64Long(1L), UInt64Long("9223372036854775816"), UInt64Long("9223372036854775813"), null)
      .toDF("a")

    val res = df
      .withColumn("a", 'a.cast(LongType))
      .withColumn("a", 'a + 1)
      .withColumn("a", 'a.cast(UInt64Type))

    res.collect() should contain theSameElementsAs Seq(
      Row(UInt64Long(2L)),
      Row(UInt64Long("9223372036854775817")),
      Row(UInt64Long("9223372036854775814")),
      Row(null)
    )
  }

  it should "join dataframes by uint64 column" in {
    val df1 = Seq(UInt64Long(1L) -> "a1", UInt64Long(2L) -> "b1").toDF("a", "b")
    val df2 = Seq(UInt64Long(1L) -> "a2", UInt64Long(2L) -> "b2", UInt64Long(3L) -> "c2").toDF("a", "c")

    df1.join(df2, Seq("a"), "outer").collect() should contain theSameElementsAs Seq(
      Row(UInt64Long(1L), "a1", "a2"),
      Row(UInt64Long(2L), "b1", "b2"),
      Row(UInt64Long(3L), null, "c2")
    )
  }

  it should "execute SortAggregate on dataframe with uint64 column" in {
    val data = Seq(
      (UInt64Long(1L), "a", 1),
      (UInt64Long(2L), "b", 1),
      (UInt64Long(3L), "c", 1)
    )
    data.toDF("a", "b", "c").write.yt(tmpPath)

    val res = withConf(org.apache.spark.sql.internal.SQLConf.USE_OBJECT_HASH_AGG, "false") {
      spark.read.yt(tmpPath).dropDuplicates("c").collect()
    }

    res.length shouldEqual 1
    res should contain oneElementOf Seq(
      Row(UInt64Long(1L), "a", 1),
      Row(UInt64Long(2L), "b", 1),
      Row(UInt64Long(3L), "c", 1)
    )
  }

}
