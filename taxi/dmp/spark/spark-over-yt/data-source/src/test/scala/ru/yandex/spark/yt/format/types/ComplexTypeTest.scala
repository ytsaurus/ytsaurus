package ru.yandex.spark.yt.format.types

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, Row, SaveMode}
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.YsonEncoder
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.common.utils.TypeUtils
import ru.yandex.spark.yt.format._
import ru.yandex.spark.yt.test.{LocalSpark, TestUtils, TmpDir}
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

import scala.collection.mutable

class ComplexTypeTest extends FlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {

  import ComplexTypeTest._
  import spark.implicits._

  private val anySchema = new TableSchema.Builder()
    .setUniqueKeys(false)
    .addValue("value", ColumnValueType.ANY)
    .build()

  "YtFormat" should "read dataset with list of long" in {
    writeTableFromYson(Seq(
      "{value = [1; 2; 3]}",
      "{value = [4; 5; 6]}"
    ), tmpPath, anySchema)

    val res = spark.read.schemaHint("value" -> ArrayType(LongType)).yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Seq(1, 2, 3)),
      Row(Seq(4, 5, 6))
    )
  }

  it should "read dataset with list of string" in {
    writeTableFromYson(Seq(
      """{value = ["a"; "b"; "c"]}"""
    ), tmpPath, anySchema)

    val res = spark.read.schemaHint("value" -> ArrayType(StringType)).yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Seq("a", "b", "c"))
    )
  }

  it should "read dataset with list of lists" in {
    writeTableFromYson(Seq(
      "{value = [[1]; [2; 3]]}",
      "{value = [[4]; #]}"
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> ArrayType(ArrayType(LongType)))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Seq(Seq(1), Seq(2, 3))),
      Row(Seq(Seq(4), null))
    )
  }

  it should "read dataset with struct of atomic" in {
    writeTableFromYson(Seq(
      """{value = {a = 1; b = "a"}}""",
      """{value = {a = 2; b = "b"}}""",
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> StructType(Seq(StructField("a", LongType), StructField("b", StringType))))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Row(1, "a")),
      Row(Row(2, "b"))
    )
  }

  it should "read dataset with struct of lists" in {
    writeTableFromYson(Seq(
      """{value = {a = [1; 2; 3]; b = ["a"; #]; c = [{a = 1; b = "a"}; {a = 2; b = "b"}]}}""",
      """{value = {a = #; b = [#; "b"]; c = [{a = 3; b = "c"}; {a = 4; b = "d"}]}}"""
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> StructType(Seq(
        StructField("a", ArrayType(LongType)),
        StructField("b", ArrayType(StringType)),
        StructField("c", ArrayType(StructType(Seq(
          StructField("a", LongType),
          StructField("b", StringType)
        ))))
      )))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Row(Seq(1, 2, 3), Seq("a", null), Seq(Row(1, "a"), Row(2, "b")))),
      Row(Row(null, Seq(null, "b"), Seq(Row(3, "c"), Row(4, "d"))))
    )
  }

  it should "read dataset with map of atomic" in {
    writeTableFromYson(Seq(
      "{value = {a = 1; b = 2}}",
      "{value = {c = 3; d = 4}}"
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> MapType(StringType, LongType))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Map("a" -> 1, "b" -> 2)),
      Row(Map("c" -> 3, "d" -> 4))
    )
  }


  it should "read dataset with map where value is a binary" in {
    writeTableFromYson(Seq(
      s"""{value = {
         |a = ${Long.MinValue};
         |b = ${Long.MaxValue};
         |c = 0;
         |d = 1234567890;
         |e = ["a";"b"];
         |f = {a = 1; b = 2; c = #};
         |}
         |}""".stripMargin,
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> MapType(StringType, BinaryType))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    val values = res.collect()
      .map { case Row(m: Map[String, Array[Byte]]) => Row(m.mapValues(_.toList)) }
    values should contain theSameElementsAs Seq(
      Row(Map(
        "a" -> YsonEncoder.encode(Long.MinValue, LongType, false).toList,
        "b" -> YsonEncoder.encode(Long.MaxValue, LongType, false).toList,
        "c" -> YsonEncoder.encode(0L, LongType, false).toList,
        "d" -> YsonEncoder.encode(1234567890L, LongType, false).toList,
        "e" -> YsonEncoder.encode(List("a", "b"), ArrayType(StringType), false).toList,
        "f" -> YsonEncoder.encode(Map("a" -> 1, "b" -> 2, "c" -> null), MapType(StringType, IntegerType), false).toList,
      ))
    )
  }

  it should "read dataset with list where value is a binary" in {
    writeTableFromYson(Seq(
      s"""{value = [${Long.MinValue};${Long.MaxValue};0;"aaa";[[1; 2]];[{b = %false};{a = %true}];1.123]}""".stripMargin,
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> ArrayType(BinaryType))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    val values = res.collect()
      .map { case Row(l: mutable.WrappedArray[Array[Byte]]) => Row(l.map(_.toList)) }

    values should contain theSameElementsAs Seq(
      Row(List(
        YsonEncoder.encode(Long.MinValue, LongType, false).toList,
        YsonEncoder.encode(Long.MaxValue, LongType, false).toList,
        YsonEncoder.encode(0L, LongType, false).toList,
        YsonEncoder.encode("aaa", StringType, false).toList.drop(2),
        YsonEncoder.encode(List(List(1, 2)), ArrayType(ArrayType(IntegerType)), false).toList,
        YsonEncoder.encode(List(Map("b" -> false), Map("a" -> true)), ArrayType(MapType(StringType, BooleanType)), false).toList,
        YsonEncoder.encode(1.123, DoubleType, false).toList,
      ))
    )
  }

  it should "read dataset with map where keys are long" in {
    writeTableFromYson(Seq(
      """{value = [[1; "2"]; [3; "4"]]}""",
      """{value = [[5; "6"]; [7; "8"]]}"""
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> MapType(LongType, StringType))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Map(1 -> "2", 3 -> "4")),
      Row(Map(5 -> "6", 7 -> "8"))
    )
  }

  it should "read dataset with map where keys are double" in {
    writeTableFromYson(Seq(
      """{value = [[1.1; 2]; [3.3; 4]]}""",
      """{value = [[5.5; 6]; [7.7; 8]]}"""
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> MapType(DoubleType, LongType))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Map(1.1 -> 2L, 3.3 -> 4L)),
      Row(Map(5.5 -> 6L, 7.7 -> 8L))
    )
  }

  it should "read dataset with map where keys are boolean" in {
    writeTableFromYson(Seq(
      """{value = [[%true; %true];  [%false; %false]]}""",
      """{value = [[%true; %false]; [%false; %true]]}"""
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> MapType(BooleanType, BooleanType))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Map(true -> true, false -> false)),
      Row(Map(true -> false, false -> true))
    )
  }

  it should "read dataset with nested maps" in {
    writeTableFromYson(Seq(
      """{value = [
        |[[[1;%true]];[[1;{a = [[1;%true]; [2;%false]]}]]];
        |[[[2;%true]];[[2;{b = [[2;%false];[3;%true]]}]]];
        |[[[3;%true]];[[3;{c = [[3;%true]; [4;%false]]}]]]
        |]}""".stripMargin,
    ), tmpPath, anySchema)

    val t = MapType(MapType(LongType, BooleanType), MapType(LongType, MapType(StringType, MapType(LongType, BooleanType))))

    val res = spark.read
      .schemaHint("value" -> t)
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(
        Map(
          Map(1 -> true) -> Map(1 -> Map("a" -> Map(1 -> true, 2 -> false))),
          Map(2 -> true) -> Map(2 -> Map("b" -> Map(2 -> false, 3 -> true))),
          Map(3 -> true) -> Map(3 -> Map("c" -> Map(3 -> true, 4 -> false))))
      ),
    )
  }

  it should "throw an exception when reading a map with non-string keys and `#` is a key" in {
    writeTableFromYson(Seq(
      """{value = [[1;1];[#;2];[3;3]]}""",
    ), tmpPath, anySchema)

    Logger.getRootLogger.setLevel(Level.OFF)
    a[SparkException] shouldBe thrownBy {
      spark.read
        .schemaHint("value" -> MapType(LongType, LongType))
        .yt(tmpPath)
        .collect()
    }
    Logger.getRootLogger.setLevel(Level.WARN)
  }

  it should "read dataset with tuples" in {
    writeTableFromYson(Seq(
      """{value = [1; "spark"; %true;]}""",
      """{value = [2; "yql"; %false;]}""",
      """{value = [3; "cpp"; #;]}""",
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> TypeUtils.tuple(LongType, StringType, BooleanType))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Row(1, "spark", true)),
      Row(Row(2, "yql", false)),
      Row(Row(3, "cpp", null))
    )
  }

  it should "read dataset with maps of tuples" in {
    writeTableFromYson(Seq(
      """{value = [[1; ["spark"; %true;]]]}""",
      """{value = [[2; ["yql"; %false;]]]}""",
      """{value = [[3; ["cpp"; #;]]]}""",
    ), tmpPath, anySchema)

    val schema = MapType(LongType, TypeUtils.tuple(StringType, BooleanType))

    val res = spark.read
      .schemaHint("value" -> schema)
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Map(1 -> Row("spark", true))),
      Row(Map(2 -> Row("yql", false))),
      Row(Map(3 -> Row("cpp", null)))
    )
  }

  it should "read dataset with tuples of maps" in {
    writeTableFromYson(Seq(
      """{value = [1; [[1.1; %true;]]]}""",
      """{value = [2; [[2.2; %false;]]]}""",
      """{value = [3; [[3.3; #;]]]}""",
    ), tmpPath, anySchema)

    val schema = TypeUtils.tuple(LongType, MapType(DoubleType, BooleanType))

    val res = spark.read
      .schemaHint("value" -> schema)
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Row(1, Map(1.1 -> true))),
      Row(Row(2, Map(2.2 -> false))),
      Row(Row(3, Map(3.3 -> null)))
    )
  }

  it should "read dataset with tuples having corrupted values" in {
    writeTableFromYson(Seq(
      """{value = [1; "spark"]}""",
      """{value = [#]}""",
      """{value = [3; "cpp"; #;]}""",
    ), tmpPath, anySchema)

    val schema = TypeUtils.tuple(LongType, StringType, BooleanType)

    val res = spark.read
      .schemaHint("value" -> schema)
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Row(1, "spark", null)),
      Row(Row(null, null, null)),
      Row(Row(3, "cpp", null))
    )
  }

  it should "read dataset with list of tuples" in {
    writeTableFromYson(Seq(
      """{value = [[1; "spark"];[2; "scala"]]}""",
      """{value = [[#]]}""",
      """{value = [[3; #];#]}""",
    ), tmpPath, anySchema)

    val schema = ArrayType(TypeUtils.tuple(LongType, StringType))

    val res = spark.read
      .schemaHint("value" -> schema)
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Seq(Row(1, "spark"), Row(2, "scala"))),
      Row(Seq(Row(null, null))),
      Row(Seq(Row(3, null), null))
    )
  }

  it should "read dataset with nested tuples" in {
    writeTableFromYson(Seq(
      """{value = [1; [2; [3; "spark"]]]}""",
      """{value = [4; [5; [6; "yql"]]]}""",
      """{value = [7; [8; [9; #]]]}""",
    ), tmpPath, anySchema)

    val schema = TypeUtils.tuple(LongType, TypeUtils.tuple(LongType, TypeUtils.tuple(LongType, StringType)))

    val res = spark.read
      .schemaHint("value" -> schema)
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Row(1, Row(2, Row(3, "spark")))),
      Row(Row(4, Row(5, Row(6, "yql")))),
      Row(Row(7, Row(8, Row(9, null)))),
    )
  }

  it should "read dataset with complex types" in {
    writeComplexTable(tmpPath)

    val schema = Encoders.product[Test].schema
    val res = spark.read.schemaHint(schema).yt(tmpPath)

    res.columns should contain theSameElementsAs schema.fieldNames
    res.as[Test].collect() should contain theSameElementsAs Seq(testRow)
  }

  it should "read some columns from dataset with complex row" in {
    writeTableFromYson(Seq(
      """{value={
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
        |}}""".stripMargin
    ), tmpPath, anySchema)
    val res = spark.read
      .schemaHint("value" -> Encoders.product[TestSmall].schema)
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.select($"value.*").as[TestSmall].collect() should contain theSameElementsAs Seq(testRowSmall)
  }

  it should "write dataset with complex types" in {
    import spark.implicits._

    Seq(
      (Seq(1, 2, 3), A(1, Some("a")), Map("1" -> 0.1)),
      (Seq(4, 5, 6), A(2, None), Map("2" -> 0.3))
    )
      .toDF("a", "b", "c").coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .yt(tmpPath)

    val res = spark.read
      .schemaHint(
        "a" -> ArrayType(LongType),
        "b" -> StructType(Seq(StructField("field1", LongType), StructField("field2", StringType))),
        "c" -> MapType(StringType, DoubleType)
      )
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(Seq(1, 2, 3), Row(1, "a"), Map("1" -> 0.1)),
      Row(Seq(4, 5, 6), Row(2, null), Map("2" -> 0.3))
    )
  }

  it should "sort map data while writing" in {
    import ru.yandex.yson.YsonTags._
    val data = Seq(
      (Some(Map("c" -> 3, "a" -> 1, "b" -> 2)), Some(Map("3" -> "c", "1" -> "a", "2" -> "b"))),
      (Some(Map.empty[String, Int]), Some(Map.empty[String, String])),
      (None, None)
    )
    val binaryData = Seq(
      (
        Some(Seq(
          BEGIN_MAP,
          BINARY_STRING, 2, 'a'.toByte, KEY_VALUE_SEPARATOR, BINARY_INT, 2,
          ITEM_SEPARATOR,
          BINARY_STRING, 2, 'b'.toByte, KEY_VALUE_SEPARATOR, BINARY_INT, 4,
          ITEM_SEPARATOR,
          BINARY_STRING, 2, 'c'.toByte, KEY_VALUE_SEPARATOR, BINARY_INT, 6,
          END_MAP
        )),
        Some(Seq(
          BEGIN_MAP,
          BINARY_STRING, 2, '1'.toByte, KEY_VALUE_SEPARATOR, BINARY_STRING, 2, 'a'.toByte,
          ITEM_SEPARATOR,
          BINARY_STRING, 2, '2'.toByte, KEY_VALUE_SEPARATOR, BINARY_STRING, 2, 'b'.toByte,
          ITEM_SEPARATOR,
          BINARY_STRING, 2, '3'.toByte, KEY_VALUE_SEPARATOR, BINARY_STRING, 2, 'c'.toByte,
          END_MAP
        ))
      ),
      (Some(Seq(BEGIN_MAP, END_MAP)), Some(Seq(BEGIN_MAP, END_MAP))),
      (None, None)
    )
    val df = data.toDF("map1", "map2")

    df.write.yt(tmpPath)

    val res = spark.read
      .yt(tmpPath)
      .select('map1.cast(BinaryType), 'map2.cast(BinaryType))
      .as[(Option[Array[Byte]], Option[Array[Byte]])]
      .collect()
      .map{case (x,y) => x.map(_.toList) -> y.map(_.toList)}

    res should contain theSameElementsAs binaryData
  }
}

object ComplexTypeTest {
  val testRow = Test(
    Map("a" -> Some(Map("aa" -> Some(1L))), "b" -> None, "c" -> Some(Map("cc" -> None))),
    Map("a" -> Some(B(Some("aa"))), "b" -> None, "c" -> Some(B(None))),
    Map("a" -> Some(Seq(Some(0.1))), "b" -> None, "c" -> Some(Seq(None))),
    Map("a" -> Some(true), "b" -> None),
    C(
      Map("a" -> Some(1L), "b" -> None),
      B(Some("aa")),
      Seq(Some(true), None),
      Some(0.1)
    ),
    Seq(Some(Map("a" -> Some(1L), "b" -> None)), None),
    Seq(Some(B(Some("aa"))), Some(B(None)), None),
    Seq(Some(Seq(Some(1L), None)), None),
    Seq(Some(0.1), None),
    Map(
      1L -> Some(Map("a" -> Some(true))),
      2L -> Some(Map("b" -> Some(false))),
      3L -> Some(Map("c" -> None)),
      4L -> None
    ),
    Map(
      Some(Map("a" -> Some(true))) -> 1L,
      Some(Map("b" -> Some(false))) -> 2L,
      Some(Map("c" -> None)) -> 3L,
    ))

  val testRowSmall = TestSmall(
    testRow.f1,
    testRow.f4,
    testRow.f7,
    testRow.f10,
    testRow.f11
  )
}
