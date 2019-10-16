package ru.yandex.inside.yt.kosher.impl.ytree.serialization

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen._
import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Properties, Test}
import ru.yandex.spark.yt.serializers.{SchemaConverter, YsonRowConverter}

class YsonDecoderUnsafeProperties extends Properties("YsonDecoderUnsafe") {
  private val eps = 0.0001

  property("decode") = forAll(genTree) { case StructValue(expected, dataType) =>
    val bytes = YsonRowConverter.serialize(expected, dataType)
    val writer = new UnsafeRowWriter(1)
    writer.reset()
    YsonDecoderUnsafe.decode(bytes, SchemaConverter.indexedDataType(dataType), writer, 0)
    val actual = writer.getRow.get(0, dataType)
    equals(expected, actual, dataType)
  }

  override def overrideParameters(p: Test.Parameters): Test.Parameters = {
    super.overrideParameters(p.withMinSuccessfulTests(1000))
  }

  def equals(expected: Any, actual: Any, dataType: DataType): Boolean = {
    dataType match {
      case _@(LongType | BooleanType) => expected == actual
      case DoubleType => Math.abs(expected.asInstanceOf[Double] - actual.asInstanceOf[Double]) < eps
      case StringType => expected.asInstanceOf[String] == actual.asInstanceOf[UTF8String].toString
      case MapType(StringType, LongType, _) =>
        val actualMapData = actual.asInstanceOf[UnsafeMapData]
        val actualMap = actualMapData.keyArray.toSeq[UTF8String](StringType).map(_.toString)
          .zip(actualMapData.valueArray.toSeq[Long](LongType)).toMap
        expected.asInstanceOf[Map[String, Long]] == actualMap
      case StructType(fields) =>
        val expectedRow = expected.asInstanceOf[Row]
        val actualRow = actual.asInstanceOf[InternalRow]
        fields.zipWithIndex.forall { case (field, index) =>
          equals(expectedRow.get(index), actualRow.get(index, field.dataType), field.dataType)
        }
      case ArrayType(elementType, _) =>
        expected.asInstanceOf[Seq[Any]]
          .zip(actual.asInstanceOf[ArrayData].toSeq[Any](elementType))
          .forall { case (e, a) =>
            equals(e, a, elementType)
          }
    }
  }

  case class StructValue(value: Any, dataType: DataType)

  val genLong = arbitrary[Long].map(StructValue(_, LongType))
  val genBoolean = arbitrary[Boolean].map(StructValue(_, BooleanType))
  val genDouble = Gen.choose(1, 1000).map(v => StructValue(v.toDouble / 1000, DoubleType))

  val genString = for {
    size <- Gen.choose(1, 100)
    chars <- Gen.listOfN(size, Gen.asciiChar)
  } yield StructValue(chars.mkString(""), StringType)

  val genMapName = for {
    size <- Gen.choose(1, 20)
    chars <- Gen.listOfN(size, Gen.alphaChar)
  } yield chars.mkString("")

  val genMap = for {
    size <- Gen.choose(1, 10)
    keys <- Gen.listOfN(size, genMapName)
    values <- Gen.listOfN(size, arbitrary[Long])
  } yield StructValue(keys.zip(values).toMap, MapType(StringType, LongType))

  val genArray = for {
    size <- Gen.choose(0, 20)
    values <- Gen.listOfN(size, genMapName)
  } yield StructValue(values, ArrayType(StringType))

  def genStruct: Gen[StructValue] = for {
    size <- Gen.choose(1, 10)
    fieldNames <- Gen.listOfN(size, genMapName).map(_.distinct)
    values <- Gen.listOfN(fieldNames.length,
      Gen.oneOf(genLong, genMap, genString, genBoolean, genDouble, genArray, lzy(genStruct)))
  } yield {
    val structType = StructType(values.zip(fieldNames).map { case (v, name) => StructField(name, v.dataType) })
    StructValue(Row.fromSeq(values.map(_.value)), structType)
  }

  def genTree: Gen[StructValue] = oneOf(genLong, genMap, genStruct, genString, genBoolean, genDouble, genArray)
}
