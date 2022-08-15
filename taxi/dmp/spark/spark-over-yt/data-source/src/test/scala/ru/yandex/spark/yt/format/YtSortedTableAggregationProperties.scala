package ru.yandex.spark.yt.format

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, CodegenObjectFactoryMode, KnownFloatingPointNormalized, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.{FakeSortedShuffleExchangeExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf.{CODEGEN_FACTORY_MODE, WHOLESTAGE_CODEGEN_ENABLED}
import org.apache.spark.sql.types._
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import ru.yandex.spark.yt.YtReader
import ru.yandex.spark.yt.format.YtSortedTableAggregationProperties._
import ru.yandex.spark.yt.format.conf.SparkYtConfiguration
import ru.yandex.spark.yt.format.optimizer.YtSortedTableMarkerRule
import ru.yandex.spark.yt.test.{LocalSpark, TestUtils, TmpDir}
import ru.yandex.type_info.TiType
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}
import ru.yandex.yt.ytclient.wire.{UnversionedRow, UnversionedValue}

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}

private class YtSortedTableAggregationProperties extends FlatSpec with Matchers with BeforeAndAfterAll
  with ScalaCheckDrivenPropertyChecks with TmpDir with LocalSpark with TestUtils {

  // 10Kb ~ 600 rows with 2 long numbers
  private val conf = Map(
    WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
    CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString,
    s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.Enabled.name}" -> "true",
    s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.UnionLimit.name}" -> "2",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.files.maxPartitionBytes" -> "10Kb",
    "spark.yt.minPartitionBytes" -> "10Kb"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.experimental.extraOptimizations = Seq(new YtSortedTableMarkerRule())
  }

  override def afterAll(): Unit = {
    spark.experimental.extraOptimizations = Nil
    super.afterAll()
  }

  private def getColumnNames(attributes: Seq[NamedExpression]): Seq[String] = {
    def parseAttribute(attribute: AttributeReference): String = {
      attribute.name
    }

    attributes.map {
      case a: AttributeReference => parseAttribute(a)
      case Alias(child, _) =>
        child match {
          case KnownFloatingPointNormalized(child) =>
            child match {
              case NormalizeNaNAndZero(child) =>
                child match {
                  case a: AttributeReference => parseAttribute(a)
                }
            }
        }
    }
  }

  private def isCorrectPlanWithRealExchange(test: AggTest, plan: SparkPlan): Boolean = {
    plan match {
      case HashAggregateExec(_, groupingExpressions1, _, _, _, _, child) =>
        child match {
          case ShuffleExchangeExec(_, child, _) =>
            child match {
              case HashAggregateExec(_, groupingExpressions2, _, _, _, _, child) =>
                child match {
                  case ProjectExec(projectList, child) =>
                    child match {
                      case BatchScanExec(output, _) =>
                        getColumnNames(groupingExpressions1) == test.groupCols &&
                          getColumnNames(groupingExpressions2) == test.groupCols &&
                          getColumnNames(projectList) == test.groupCols &&
                          getColumnNames(output) == test.groupCols
                      case _ => false
                    }
                  case _ => false
                }
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
  }

  private def isCorrectPlanWithFakeExchange(test: AggTest, plan: SparkPlan): Boolean = {
    plan match {
      case HashAggregateExec(_, groupingExpressions1, _, _, _, _, child) =>
        child match {
          case HashAggregateExec(_, groupingExpressions2, _, _, _, _, child) =>
            child match {
              case shuffle: FakeSortedShuffleExchangeExec =>
                shuffle.child match {
                  case ProjectExec(projectList, child) =>
                    child match {
                      case BatchScanExec(output, _) =>
                        getColumnNames(groupingExpressions1) == test.groupCols &&
                          getColumnNames(groupingExpressions2) == test.groupCols &&
                          getColumnNames(projectList) == test.groupCols &&
                          getColumnNames(output) == test.groupCols
                      case _ => false
                    }
                  case _ => false
                }
            }
          case _ => false
        }
      case _ => false
    }
  }

  private def isCorrectPlan(test: AggTest, plan: SparkPlan): Boolean = {
    isCorrectPlanWithRealExchange(test, plan) || isCorrectPlanWithFakeExchange(test, plan)
  }

  it should "optimize" in {
    withConfs(conf) {
      sys.props += "spark.testing" -> "true"
      forAll(genTest, minSuccessful(10)) {
        case test@AggTest(schema, data, keys, groupCols) =>
          beforeEach()
          val sortedData = data.sortWith { case (l, r) => compareRows(l, r) }
          val expected = sortedData
            .groupBy(s => s.take(groupCols.length)).mapValues(_.length).toSeq
            .map { case (key, v) => key :+ v }
          writeTableFromURow(sortedData.map(getUnversionedRow(schema, _)), tmpPath, getTableSchema(schema, keys))
          val query = spark.read.yt(tmpPath).groupBy(groupCols.map(col): _*).count()
          val res = query.collect()
          isCorrectPlan(test, query.queryExecution.executedPlan) shouldBe true
          res should contain theSameElementsAs expected.map(Row.fromSeq)
          afterEach()
      }
    }
  }
}

object YtSortedTableAggregationProperties {
  private val genLong = Gen.choose(1L, 1000L)
  private val genDouble = Gen.choose(1, 1000).map(v => v.toDouble / 1000)
  private val genString = for {
    size <- Gen.choose(1, 10)
    chars <- Gen.listOfN(size, Gen.asciiChar)
  } yield chars.mkString("")

  private val genName = for {
    size <- Gen.choose(1, 5)
    chars <- Gen.listOfN(size, Gen.alphaChar)
  } yield chars.mkString("")
  private val genDataType = Gen.oneOf(StringType, LongType, DoubleType)

  private def getDatatypeGen(dt: DataType): Gen[Any] = dt match {
    case StringType => genString
    case LongType => genLong
    case DoubleType => genDouble
  }

  def compareRows(l: Seq[Any], r: Seq[Any]): Boolean = {
    l.zip(r).find {
      case (a, b) => a != b
    }.exists {
      case (a: String, b: String) => a < b
      case (a: Long, b: Long) => a < b
      case (a: Double, b: Double) => a < b
    }
  }

  def getTiType(dt: DataType): TiType = dt match {
    case StringType => TiType.string()
    case LongType => TiType.int64()
    case DoubleType => TiType.doubleType()
  }

  private def getUnversionedValue(i: Int, dt: DataType, value: Any): UnversionedValue = dt match {
    case StringType =>
      new UnversionedValue(i, ColumnValueType.STRING, false, value.asInstanceOf[String].getBytes)
    case LongType =>
      new UnversionedValue(i, ColumnValueType.INT64, false, value.asInstanceOf[Long])
    case DoubleType =>
      new UnversionedValue(i, ColumnValueType.DOUBLE, false, value.asInstanceOf[Double])
  }

  def getUnversionedRow(schema: Schema, row: Seq[Any]): UnversionedRow = {
    new UnversionedRow(
      schema.cols.zipWithIndex.map {
        case ((_, dt), i) => getUnversionedValue(i, dt, row(i))
      }.asJava
    )
  }

  def getTableSchema(schema: Schema, keys: Seq[String]): TableSchema = {
    schema.cols.foldLeft(new TableSchema.Builder()) {
      case (builder, (name, dt)) =>
        if (keys.contains(name)) {
          builder.addKey(name, getTiType(dt))
        } else {
          builder.addValue(name, getTiType(dt))
        }
    }.setUniqueKeys(false).build()
  }

  case class Schema(cols: Seq[(String, DataType)])

  private def genSchema: Gen[Schema] = {
    for {
      size <- Gen.choose(1, 4)
      names <- Gen.listOfN(size, genName).retryUntil(l => l.toSet.size == l.size) // all names must be unique
      dataTypes <- Gen.listOfN(size, genDataType)
    } yield {
      Schema(names.zip(dataTypes))
    }
  }

  private def genRawRow(schema: Schema): Gen[Seq[Any]] = {
    Gen.sequence(
      schema.cols.map {
        case (_, dt) => getDatatypeGen(dt)
      }
    ).map(_.asScala.toSeq)
  }

  private def genData(schema: Schema): Gen[Seq[Seq[Any]]] = {
    for {
      size <- Gen.choose(1000, 10000)
      rawData <- Gen.listOfN(size, genRawRow(schema))
    } yield {
      rawData
    }
  }

  case class AggTest(schema: Schema, data: Seq[Seq[Any]], keys: Seq[String], groupCols: Seq[String])

  def genTest: Gen[AggTest] = {
    for {
      schema <- genSchema
      data <- genData(schema)
      keysNumber <- Gen.choose(1, schema.cols.length)
      groupColsNumber <- Gen.choose(1, schema.cols.length)
    } yield {
      val colNames = schema.cols.map { case (name, _) => name }
      val keys = colNames.take(keysNumber)
      val groupCols = colNames.take(groupColsNumber)
      AggTest(schema, data, keys, groupCols)
    }
  }
}
