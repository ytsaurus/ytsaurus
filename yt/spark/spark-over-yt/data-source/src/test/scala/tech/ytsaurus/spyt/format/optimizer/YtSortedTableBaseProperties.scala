package tech.ytsaurus.spyt.format.optimizer

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, CodegenObjectFactoryMode, Expression, KnownFloatingPointNormalized, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.internal.SQLConf.{CODEGEN_FACTORY_MODE, WHOLESTAGE_CODEGEN_ENABLED}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, LongType, StringType}
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import YtSortedTableBaseProperties.{Source, compareRows, getTableSchema, getUnversionedRow}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedValue}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.typeinfo.TiType

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}

private class YtSortedTableBaseProperties extends FlatSpec with Matchers with BeforeAndAfterAll
  with ScalaCheckDrivenPropertyChecks with TmpDir with LocalSpark with TestUtils {

  // 10Kb ~ 600 rows with 2 long numbers
  protected val conf = Map(
    WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
    CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString,
    s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.Enabled.name}" -> "true",
    s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.UnionLimit.name}" -> "2",
    s"spark.yt.${SparkYtConfiguration.Read.PlanOptimizationEnabled.name}" -> "true",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.sql.files.maxPartitionBytes" -> "10Kb",
    "spark.yt.minPartitionBytes" -> "10Kb"
  )

  protected def writeSortedData(source: Source, path: String): Seq[Seq[Any]] = {
    val sortedData = source.data.data.sortWith { case (l, r) => compareRows(l, r) }
    writeTableFromURow(
      sortedData.map(getUnversionedRow(source.schema, _)),
      path, getTableSchema(source.schema, source.keys))
    sortedData
  }
}

object YtSortedTableBaseProperties {
  private val genLong = Gen.choose(1L, 100L)
  private val genDouble = Gen.choose(1, 100).map(v => v.toDouble / 1000)
  private val genBoolean = Gen.choose(1, 2).map(_ == 2)
  private val genString = for {
    size <- Gen.choose(1, 3)
    chars <- Gen.listOfN(size, Gen.alphaLowerChar)
  } yield chars.mkString("")

  private val genName = for {
    size <- Gen.choose(1, 5)
    chars <- Gen.listOfN(size, Gen.alphaLowerChar)
  } yield chars.mkString("")
  private val genDataType = Gen.oneOf(StringType, LongType, BooleanType)

  private def getDatatypeGen(dt: DataType): Gen[Any] = dt match {
    case StringType => genString
    case LongType => genLong
    case BooleanType => genBoolean
  }

  def compareRows(l: Seq[Any], r: Seq[Any]): Boolean = {
    l.zip(r).find {
      case (a, b) => a != b
    }.exists {
      case (a: String, b: String) => a < b
      case (a: Long, b: Long) => a < b
      case (a: Double, b: Double) => a < b
      case (a: Boolean, b: Boolean) => a < b
    }
  }

  private def getTiType(dt: DataType): TiType = dt match {
    case StringType => TiType.string()
    case LongType => TiType.int64()
    case DoubleType => TiType.doubleType()
    case BooleanType => TiType.bool()
  }

  private def getUnversionedValue(i: Int, dt: DataType, value: Any): UnversionedValue = dt match {
    case StringType =>
      new UnversionedValue(i, ColumnValueType.STRING, false, value.asInstanceOf[String].getBytes)
    case LongType =>
      new UnversionedValue(i, ColumnValueType.INT64, false, value.asInstanceOf[Long])
    case DoubleType =>
      new UnversionedValue(i, ColumnValueType.DOUBLE, false, value.asInstanceOf[Double])
    case BooleanType =>
      new UnversionedValue(i, ColumnValueType.BOOLEAN, false, value.asInstanceOf[Boolean])
  }

  def getUnversionedRow(schema: Schema, row: Seq[Any]): UnversionedRow = {
    new UnversionedRow(
      schema.getColumnTypes().zipWithIndex.map {
        case (dt, i) => getUnversionedValue(i, dt, row(i))
      }.asJava
    )
  }

  def getTableSchema(schema: Schema, keys: Seq[String]): TableSchema = {
    schema.cols.foldLeft(TableSchema.builder()) {
      case (builder, (name, dt)) =>
        if (keys.contains(name)) {
          builder.addKey(name, getTiType(dt))
        } else {
          builder.addValue(name, getTiType(dt))
        }
    }.setUniqueKeys(false).build()
  }

  case class Schema(cols: Seq[(String, DataType)]) {
    def getColumnNames(number: Int = cols.length): Seq[String] = {
      cols.take(number).map { case (name, _) => name }
    }

    def getColumnTypes(number: Int = cols.length): Seq[DataType] = {
      cols.take(number).map { case (_, dt) => dt }
    }
  }

  case class Data(data: Seq[Seq[Any]])

  case class Source(schema: Schema, keys: Seq[String], data: Data)

  def genSchema(minSize: Int = 2, maxSize: Int = 5): Gen[Schema] = {
    for {
      size <- Gen.choose(minSize, maxSize)
      names <- Gen.listOfN(size, genName).retryUntil(l => l.toSet.size == l.size) // all names must be unique
      dataTypes <- Gen.listOfN(size, genDataType)
    } yield {
      Schema(names.zip(dataTypes))
    }
  }

  private def genRawRow(schema: Schema): Gen[Seq[Any]] = {
    Gen.sequence(schema.cols.map { case (_, dt) => getDatatypeGen(dt) }).map(_.asScala.toSeq)
  }

  def genData(schema: Schema): Gen[Data] = {
    for {
      size <- Gen.choose(1000, 2000)
      rawData <- Gen.listOfN(size, genRawRow(schema))
    } yield {
      Data(rawData)
    }
  }

  def genPrefixColumns(schema: Schema): Gen[Seq[String]] = {
    for {
      keysNumber <- Gen.choose(1, schema.cols.length)
    } yield {
      schema.getColumnNames(keysNumber)
    }
  }

  def getColumnNames(attributes: Seq[Expression]): Seq[String] = {
    attributes.map {
      case a: AttributeReference => a.name
      case Alias(KnownFloatingPointNormalized(NormalizeNaNAndZero(a: AttributeReference)), _) => a.name
      case KnownFloatingPointNormalized(NormalizeNaNAndZero(a: AttributeReference)) => a.name
    }
  }
}


