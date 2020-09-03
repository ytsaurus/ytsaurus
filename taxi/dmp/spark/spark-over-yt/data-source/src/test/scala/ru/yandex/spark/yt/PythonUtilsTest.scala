package ru.yandex.spark.yt

import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.test.{LocalSpark, TmpDir}

class PythonUtilsTest extends FlatSpec with Matchers with LocalSpark with TmpDir {

  behavior of "PythonUtils"

  import spark.implicits._

  it should "serializeColumnToYson" in {
    val df = Seq(B(A("1", "2")), B(A("3", "4"))).toDF()
    val res = PythonUtils
      .serializeColumnToYson(df, "value", "ser_value", skipNulls = true)

   res.select("ser_value").as[Array[Byte]].collect() should contain theSameElementsAs Seq(
     Array(123,1,2,97,61,1,2,49,59,1,2,98,61,1,2,50,125).map(_.toByte),
     Array(123,1,2,97,61,1,2,51,59,1,2,98,61,1,2,52,125).map(_.toByte),
   )
  }

}

case class A(a: String, b: String)
case class B(value: A)
