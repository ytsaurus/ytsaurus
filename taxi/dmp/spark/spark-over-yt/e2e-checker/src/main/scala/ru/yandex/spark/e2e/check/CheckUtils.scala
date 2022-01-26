package ru.yandex.spark.e2e.check

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.e2e.check.CheckRowsUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ru.yandex.spark.e2e.check.GroupedIterator._
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.CompoundClient

import scala.collection.mutable
import scala.util.Try

object CheckUtils {
  private val hashCodeTypes = Set(StringType, CharType, ByteType, ShortType, IntegerType, LongType, BooleanType)
  class CheckRow(val seq: Seq[Any], schema: Seq[StructField]) {
    override def hashCode(): Int = seq.zip(schema).collect {
      case (value, dt) if hashCodeTypes.contains(dt) => value
    }.hashCode()

    override def equals(obj: Any): Boolean = {
      obj match {
        case r: CheckRow => CheckRowsUtils.checkRows(seq, r.seq, schema)
        case _ => false
      }
    }
  }
  def compareRows(actual: Seq[CheckRow], expected: Seq[CheckRow]): Seq[Row] = {
    val expectedMap = mutable.HashMap.empty[CheckRow, Int]
    expected.groupBy(identity).foreach { case (exp, seq) =>
      expectedMap += exp -> seq.length
    }
    actual.flatMap { act =>
      expectedMap.get(act) match {
        case Some(count) if count > 0 =>
          expectedMap(act) -= 1
          None
        case _ =>
          Some(Row.fromSeq("actual" +: act.seq))
      }
    } ++ expectedMap.filter(_._2 > 0).keys.map(exp => Row.fromSeq("expected" +: exp.seq))
  }

  def checkData(actual: DataFrame, expected: DataFrame, keys: Seq[String], uniqueKeys: Boolean = false): Option[DataFrame] = {
    val preparedActual = actual.withColumn("info", lit("actual")).select("info", expected.columns: _*)
    val preparedExpected = expected.withColumn("info", lit("expected")).select("info", expected.columns: _*)

    val resultSchema = preparedActual.schema

    val res = preparedActual
      .union(preparedExpected)
      .repartition(keys.map(col): _*)
      .sortWithinPartitions(keys.map(col): _*)
      .mapPartitions { rows =>

        rows.groupedBy(row => keys.map(row.getAs[Any])).flatMap { groupRows =>
          val list = groupRows.toList
          val actual = list.filter(_.getAs[String]("info") == "actual").map(_.toSeq.drop(1))
          val expected = list.filter(_.getAs[String]("info") == "expected").map(_.toSeq.drop(1))
          if (uniqueKeys) {
            if (actual.isEmpty) {
              Seq(Row.fromSeq("expected" +: expected.head))
            } else if (expected.isEmpty) {
              Seq(Row.fromSeq("actual" +: actual.head))
            } else if (actual.length > 1) {
              Row.fromSeq("expected" +: expected.head) +: actual.map(a => Row.fromSeq("actual" +: a))
            } else {
              val success = CheckRowsUtils.checkRows(actual.head, expected.head, resultSchema.drop(1))
              if (!success) {
                Seq(Row.fromSeq("expected" +: expected.head), Row.fromSeq("actual" +: actual.head))
              } else Seq()
            }
          } else {
            val rowSchema = resultSchema.drop(1)
            compareRows(actual.map(new CheckRow(_, rowSchema)),
              expected.map(new CheckRow(_, rowSchema)))
          }
        }
      }(RowEncoder(resultSchema))

    Some(res.cache()).filter(_.count() > 0)
  }

  private def formatFieldDiscrepancy(actual: StructField, expected: StructField): String = {
    val builder = Seq.newBuilder[String]
    if (actual.dataType != expected.dataType) {
      builder += s"expected data type ${expected.dataType}, but actual data type was ${actual.dataType}"
    }
    if (actual.nullable != expected.nullable) {
      builder += s"expected nullable ${expected.nullable}, but actual nullable was ${actual.dataType}"
    }
    if (actual.metadata != expected.metadata) {
      builder += s"expected metadata ${expected.metadata}, but actual metadata was ${actual.metadata}"
    }

    builder.result().mkString(", ")
  }

  def checkSchema(actual: StructType, expected: StructType): Map[String, String] = {
    val actualMap = actual.map(f => f.name -> f).toMap
    val expectedMap = expected.map(f => f.name -> f).toMap
    actualMap.flatMap { case (name, actualField) =>
      expectedMap.get(name) match {
        case Some(expectedField) if expectedField == actualField => None
        case Some(expectedField) => Some(name -> formatFieldDiscrepancy(actualField, expectedField))
        case None => Some(name -> "unexpected field")
      }
    } ++ expectedMap.flatMap { case (name, _) =>
      actualMap.get(name) match {
        case None => Some(name -> "expected field not found")
        case _ => None
      }
    }
  }

  def checkData(actualPath: String,
                expectedPath: String,
                keys: Seq[String],
                uniqueKeys: Boolean)
               (implicit yt: CompoundClient, spark: SparkSession): CheckResult = {
    val res = if (!YtWrapper.exists(actualPath)) {
      CheckResult.Empty
    } else {
      import ru.yandex.spark.yt._
      val actual = spark.read.yt(actualPath)
      val expected = spark.read.yt(expectedPath)
      val schemaMap = checkSchema(actual.schema, expected.schema)
      if (schemaMap.nonEmpty) {
        CheckResult.WrongSchema(schemaMap)
      } else {
        checkData(actual, expected, keys, uniqueKeys) match {
          case Some(df) => CheckResult.WrongData(Some(df))
          case None => CheckResult.Ok
        }
      }
    }

    res
  }

  def checkDataset(actualPath: String,
                   expectedPath: String,
                   keys: Seq[String],
                   uniqueKeys: Boolean)
                  (implicit yt: CompoundClient, spark: SparkSession): CheckResult = {
    Try(checkData(actualPath, expectedPath, keys, uniqueKeys))
      .recover {
        case e: Throwable => CheckResult.UnexpectedError(e.getMessage)
      }
      .get
  }
}
