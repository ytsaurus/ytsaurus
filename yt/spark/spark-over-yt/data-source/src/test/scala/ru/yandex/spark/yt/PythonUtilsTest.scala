package ru.yandex.spark.yt

import org.apache.spark.sql.yson.YsonBinary
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.test.{LocalSpark, TmpDir}

class PythonUtilsTest extends FlatSpec with Matchers with LocalSpark with TmpDir {

  behavior of "PythonUtils"

  import spark.implicits._

  it should "serializeColumnToYson" in {
    val df = Seq(
      PythonUtilsTestB(PythonUtilsTestA("1", "2")),
      PythonUtilsTestB(PythonUtilsTestA("3", "4"))
    ).toDF()
    val res = PythonUtils
      .serializeColumnToYson(df, "value", "ser_value", skipNulls = true)

   res.map(_.getAs[YsonBinary]("ser_value").bytes).collect() should contain theSameElementsAs Seq(
     Array(123,1,2,97,61,1,2,49,59,1,2,98,61,1,2,50,125).map(_.toByte),
     Array(123,1,2,97,61,1,2,51,59,1,2,98,61,1,2,52,125).map(_.toByte),
   )
  }
}

case class PythonUtilsTestA(a: String, b: String)
case class PythonUtilsTestB(value: PythonUtilsTestA)
