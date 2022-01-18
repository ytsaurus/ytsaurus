package ru.yandex.spark.yt.format.types

import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtUtils
import org.apache.spark.sql.{DataFrameReader, Row}
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.common.Decimal.textToBinary
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.format.conf.SparkYtConfiguration.Read.ParsingTypeV3
import ru.yandex.spark.yt.format.conf.YtTableSparkSettings
import ru.yandex.spark.yt.serializers.SchemaConverter.MetadataFields
import ru.yandex.spark.yt.serializers.YtLogicalType
import ru.yandex.spark.yt.test.{LocalSpark, TestUtils, TmpDir}
import ru.yandex.type_info.StructType.Member
import ru.yandex.type_info.TiType
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}
import ru.yandex.yt.ytclient.wire.{UnversionedRow, UnversionedValue}

class ComplexTypeV3Test extends FlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {
  import spark.implicits._

  private def codeListImpl(list: Seq[Any], transformer: (YTreeBuilder, Int, Any) => Unit): Array[Byte] = {
    val builder = new YTreeBuilder
    builder.onBeginList()
    list.zipWithIndex.foreach {
      case (value, index) =>
        builder.onListItem()
        transformer(builder, index, value)
    }
    builder.onEndList()
    builder.build.toBinary
  }

  private def codeDictImpl(map: Map[String, Any]): Array[Byte] = {
    val builder = new YTreeBuilder
    builder.onBeginMap()
    map.foreach {
      case (key, value) =>
        builder.key(key)
        builder.value(value)
    }
    builder.onEndMap()
    builder.build.toBinary
  }

  private def codeList(list: Seq[Any]): Array[Byte] = {
    codeListImpl(list,
      (builder, _, value) =>
        builder.value(value)
    )
  }

  private def codeUList(cVType: ColumnValueType, list: Seq[Any]): Array[Byte] = {
    codeListImpl(list,
      (builder, index, value) =>
        new UnversionedValue(index, cVType, false, value)
          .writeTo(builder)
    )
  }

  private def packToRow(value: Any,
                        cVType: ColumnValueType = ColumnValueType.COMPOSITE): UnversionedRow = {
    new UnversionedRow(java.util.List.of[UnversionedValue](
      new UnversionedValue(0, cVType, false, value)
    ))
  }

  // TODO put in TestUtils
  private def testEnabledAndDisabledArrow(f: DataFrameReader => Unit): Unit = {
    f(spark.read.enableArrow)
    f(spark.read.disableArrow)
  }

  it should "read optional from yt" in {
    val data = Seq(Some(1L), Some(2L), None)

    writeTableFromYson(
      data.map(
        d => s"""{ optional = ${d.map(_.toString).getOrElse("#")} }"""),
      tmpPath, new TableSchema.Builder()
        .setUniqueKeys(false)
        .addValue("optional", TiType.optional(TiType.int64()))
        .build()
    )

    val res = spark.read.yt(tmpPath)
    res.collect() should contain theSameElementsAs data.map(x => Row(x.orNull))
  }

  it should "read decimal from yt" in {
    val precision = 3
    val scale = 2
    val data = Seq("1.23", "0.21")
    val byteDecimal = data.map(x => textToBinary(x, precision, scale))
    writeTableFromURow(byteDecimal.map(x => packToRow(x, ColumnValueType.STRING)), tmpPath,
      new TableSchema.Builder().setUniqueKeys(false).addValue("a", TiType.decimal(precision, scale)).build())

    withConf(s"spark.yt.${ParsingTypeV3.name}", "true") {
      testEnabledAndDisabledArrow { reader =>
        val res = reader.yt(tmpPath)
        res.collect().map(x => x.getDecimal(0).toString) should contain theSameElementsAs data
      }
    }
  }

  it should "read array from yt" in {
    val data = Seq(Seq(1L, 2L), Seq(3L, 4L, 5L))
    writeTableFromURow(
      data.map(x => packToRow(codeList(x))), tmpPath, new TableSchema.Builder()
        .setUniqueKeys(false)
        .addValue("array", TiType.list(TiType.int64()))
        .build()
    )

    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
    res.collect() should contain theSameElementsAs data.map(Row(_))
  }

  private def codeDictLikeList[T](map: Map[T, Any]): Array[Byte] = {
    codeUList(ColumnValueType.COMPOSITE, map.map { case (a, b) => codeList(Seq(a, b)) }.toSeq)
  }

  it should "read map from yt" in {
    val data = Seq(Map("1" -> true), Map("3" -> true, "4" -> false))
    writeTableFromURow(
      data.map(x => packToRow(codeDictLikeList(x))), tmpPath, new TableSchema.Builder()
        .setUniqueKeys(false)
        .addValue("map", TiType.dict(TiType.string(), TiType.bool()))
        .build()
    )

    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
    res.collect() should contain theSameElementsAs data.map(Row(_))
  }

  private def codeTestStruct(struct: TestStruct): Array[Byte] = {
    codeList(Seq[Any](struct.d, struct.s))
  }

  it should "read struct from yt" in {
    val schema = new TableSchema.Builder()
      .setUniqueKeys(false)
      .addValue("struct",
        TiType.struct(
          new Member("d", TiType.doubleType()),
          new Member("s", TiType.string())
        ))
      .build()
    val data = Seq(TestStruct(0.2, "ab"), TestStruct(0.9, "d"))

    writeTableFromURow(
      data.map(x => packToRow(codeTestStruct(x))), tmpPath, schema
    )

    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
    res.collect() should contain theSameElementsAs data.map(x => Row(Row(x.d, x.s)))
  }

  it should "read tuple from yt" in {
    val data: Seq[Array[Any]] = Seq(Array[Any](99L, 0.3), Array[Any](128L, 1.0))
    writeTableFromURow(
      data.map { x => packToRow(codeList(x)) }, tmpPath, new TableSchema.Builder()
        .setUniqueKeys(false)
        .addValue("tuple",
          TiType.tuple(
            TiType.int64(),
            TiType.doubleType()
          ))
        .build()
    )

    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
    res.collect() should contain theSameElementsAs data.map(x => Row(Row(x: _*)))
  }

  it should "read tagged from yt" in {
    writeTableFromYson(
      Seq("{ tagged = 1 }", "{ tagged = 2 }"), tmpPath,
      new TableSchema.Builder()
        .setUniqueKeys(false)
        .addValue("tagged",
          TiType.tagged(
            TiType.int64(),
            "main"
          ))
        .build()
    )

    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
    res.schema shouldBe StructType(Seq(
      StructField("tagged", LongType, nullable = true,
        new MetadataBuilder()
          .putString(MetadataFields.TAG, "main").putString(MetadataFields.ORIGINAL_NAME, "tagged")
          .putLong(MetadataFields.KEY_ID, -1).build())
    ))
    res.collect() should contain theSameElementsAs Seq(Row(1L), Row(2L))
  }

  it should "read variant over tuple from yt" in {
    val data: Seq[Seq[Any]] = Seq(Seq(null, 0.3), Seq("s", null))
    writeTableFromURow(
      Seq(packToRow(codeList(Array[Any](1L, data(0)(1))), ColumnValueType.COMPOSITE),
        packToRow(codeList(Array[Any](0L, data(1)(0))), ColumnValueType.COMPOSITE)),
      tmpPath, new TableSchema.Builder()
        .setUniqueKeys(false)
        .addValue("variant",
          TiType.variantOverTuple(
            TiType.string(),
            TiType.doubleType()
          ))
        .build()
    )

    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
    res.collect() should contain theSameElementsAs data.map(x => Row(Row(x: _*)))
  }

  it should "read variant over struct with positional view from yt" in {
    val data: Seq[Seq[Any]] = Seq(Seq(null, 0.3), Seq("t", null))
    writeTableFromURow(
      Seq(packToRow(codeList(Array[Any](1L, data(0)(1))), ColumnValueType.COMPOSITE),
        packToRow(codeList(Array[Any](0L, data(1)(0))), ColumnValueType.COMPOSITE)),
      tmpPath, new TableSchema.Builder()
        .setUniqueKeys(false)
        .addValue("variant",
          TiType.variantOverStruct(java.util.List.of(
            new Member("s", TiType.string()),
            new Member("d", TiType.doubleType())
          )))
        .build()
    )

    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
    res.collect() should contain theSameElementsAs data.map(x => Row(Row(x: _*)))
  }

  it should "write decimal to yt" in {
    import spark.implicits._
    val data = Seq(BigDecimal("1.23"), BigDecimal("0.21"))
    data
      .toDF("a").coalesce(1)
      .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(tmpPath)

    testEnabledAndDisabledArrow { reader =>
      val res = reader.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

      res.columns should contain theSameElementsAs Seq("a")
      res.collect().map(x => x.getDecimal(0).toString) should contain theSameElementsAs Seq(
        "1.230000000000000",
        "0.210000000000000"
      )
    }
  }

  it should "write array to yt" in {
    import spark.implicits._
    val data = Seq(Seq(1, 2, 3), Seq(4, 5, 6))
    data
      .toDF("a").coalesce(1)
      .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(tmpPath)

    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("a")
    res.select("a").collect() should contain theSameElementsAs data.map(Row(_))
  }

  it should "write map to yt" in {
    import spark.implicits._
    val data = Seq(
      Map("spark" -> Map(0 -> 3.14, 1 -> 2.71), "over" -> Map(2 -> -1.0)),
      Map("yt" -> Map(5 -> 5.0)))
    data
      .toDF("a").coalesce(1)
      .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(tmpPath)

    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("a")
    res.collect() should contain theSameElementsAs data.map(Row(_))
  }

  it should "write struct to yt" in {
    import spark.implicits._
    val data = Seq(TestStruct(1.0, "a"), TestStruct(3.2, "b"))
    data.map(Some(_))
      .toDF("a").coalesce(1)
      .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(tmpPath)


    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("a")
    res.collect() should contain theSameElementsAs data.map(x => Row(Row.fromTuple(x)))
  }

  it should "write tuple to yt" in {
    import spark.implicits._
    val data = Seq((1, "a"), (3, "c"))
    data.map(Some(_))
      .toDF("a").coalesce(1)
      .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(tmpPath)


    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("a")
    res.collect() should contain theSameElementsAs data.map(x => Row(Row.fromTuple(x)))
  }

  it should "write variant over tuple from yt" in {
    val data = Seq(Tuple2(Some("1"), None), Tuple2(None, Some(2.0)))
    val nullableData = Seq(Tuple2("1", null), Tuple2(null, 2.0))
    data.map(Some(_))
      .toDF("a").coalesce(1).write
      .schemaHint(Map("a" ->
        YtLogicalType.VariantOverTuple(Seq(YtLogicalType.String, YtLogicalType.Double))))
      .option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(tmpPath)


    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
    res.collect() should contain theSameElementsAs nullableData.map(x => Row(Row.fromTuple(x)))
  }

  it should "write variant over struct with positional view from yt" in {
    val data = Seq(TestVariant(None, Some("2.0")), TestVariant(Some(1), None))
    val nullableData = Seq(Tuple2(null, "2.0"), Tuple2(1, null))
    data.map(Some(_))
      .toDF("a").coalesce(1).write
      .schemaHint(Map("a" ->
        YtLogicalType.VariantOverStruct(Seq(("i", YtLogicalType.Int32), ("s", YtLogicalType.String)))))
      .option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(tmpPath)


    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
    res.collect() should contain theSameElementsAs nullableData.map(x => Row(Row.fromTuple(x)))
  }

  it should "write combined complex types" in {
    val data = Seq(
      (Map(1 -> TestStructHard(2, Some(Seq(TestStruct(3.0, "4"), TestStruct(5.0, "6"))))), "a"),
      (Map(7 -> TestStructHard(0, None)), "b")
    )
    data.map(Some(_))
      .toDF("a").coalesce(1)
      .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(tmpPath)


    val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("a")
    res.collect() should contain theSameElementsAs Seq(
      Row(Row(Map(1 -> Row(2, Seq(Row(3.0, "4"), Row(5.0, "6")))), "a")),
      Row(Row(Map(7 -> Row(0, null)), "b"))
    )
  }
}

case class TestStruct(d: Double, s: String)

case class TestVariant(i: Option[Int], s: Option[String])

case class TestStructHard(v: Int, l: Option[Seq[TestStruct]])

