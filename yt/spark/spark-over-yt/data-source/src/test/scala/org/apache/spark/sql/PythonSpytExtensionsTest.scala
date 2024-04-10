package org.apache.spark.sql

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.sql.api.python.PythonSQLUtils
import org.apache.spark.sql.types.{DataType, IntegerType, Metadata, StructField, StructType}
import org.apache.spark.sql.yson.{UInt64Long, UInt64Type}
import org.json4s.JsonAST.JString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.test.LocalSpark


class PythonSpytExtensionsTest extends AnyFlatSpec with Matchers with LocalSpark {
  import spark.implicits._

  "PythonSQLUtils" should "parse dataframe schema from python API containing uint64 type" in {
    PythonSQLUtils.parseDataType("uint64") shouldBe UInt64Type
    PythonSQLUtils.parseDataType("UInt64") shouldBe UInt64Type
    PythonSQLUtils.parseDataType("UINT64") shouldBe UInt64Type
  }

  "DataType" should "parse uint64 data type" in {
    DataType.parseDataType(JString("uint64")) shouldBe UInt64Type
  }

  "SerDeUtil.pythonToJava" should "parse pickled uint64 data" in {
    val data = Seq(
      //[(1, 1)]
      Array[Byte](-128, 5, -107, 10, 0, 0, 0, 0, 0, 0, 0, 93, -108, 75, 1, 75, 1, -122, -108, 97, 46),
      //[(2, 2), (3, 3)]
      Array[Byte](-128, 5, -107, 17, 0, 0, 0, 0, 0, 0, 0, 93, -108, 40, 75, 2, 75, 2, -122, -108, 75,
        3, 75, 3, -122, -108, 101, 46),
      //[(4, -9223372036854775803)]
      Array[Byte](-128, 5, -107, 18, 0, 0, 0, 0, 0, 0, 0, 93, -108, 75, 4, -118, 8, 5, 0, 0, 0,
        0, 0, 0, -128, -122, -108, 97, 46),
      //[(5, -9223372036854775800), (6, -1)]
      Array[Byte](-128, 5, -107, 28, 0, 0, 0, 0, 0, 0, 0, 93, -108, 40, 75, 5, -118, 8, 8, 0, 0, 0, 0, 0, 0,
        -128, -122, -108, 75, 6, 74, -1, -1, -1, -1, -122, -108, 101, 46),
      //[(7, None)]
      Array[Byte](-128, 4, -107, 9, 0, 0, 0, 0, 0, 0, 0, 93, -108, 75, 7, 78, -122, -108, 97, 46)
    )

    val schemaStr = "id int, value uint64"
    val schemaJson = StructType.fromDDL(schemaStr).json


    // here we are reproducing steps from session.py _create_dataframe method
    val rddPickled = spark.sparkContext.parallelize(data).toJavaRDD()
    val javaObjectRdd = SerDeUtil.pythonToJava(rddPickled, batched = true)
    val jrdd = SerDeUtil.toJavaArray(javaObjectRdd).asInstanceOf[JavaRDD[Array[Any]]]
    val df = spark.applySchemaToPythonRDD(jrdd.rdd, schemaJson)

    df.schema.fields.map(_.copy(metadata = Metadata.empty)) should contain theSameElementsInOrderAs Seq(
      StructField("id", IntegerType),
      StructField("value", UInt64Type)
    )

    df.select("id").as[Int].collect() should contain theSameElementsAs Seq(1, 2, 3, 4, 5, 6, 7)
    df.select("value").as[UInt64Long].collect() should contain theSameElementsAs Seq(
      UInt64Long(1L), UInt64Long(2L), UInt64Long(3L), UInt64Long("9223372036854775813"),
      UInt64Long("9223372036854775816"), UInt64Long("18446744073709551615"), null
    )
  }
}
