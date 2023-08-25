package tech.ytsaurus.spyt.format

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Row, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.exceptions._
import tech.ytsaurus.spyt.serializers.{InternalRowSerializer, SchemaConverter}
import tech.ytsaurus.spyt.serializers.SchemaConverter.{Sorted, Unordered}
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import scala.concurrent.duration._
import scala.language.postfixOps

class YtDynamicTableWriterTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers {
  import YtDynamicTableWriterTest._

  override val numExecutors = 8

  "YtDynamicTableWriter" should "write lots of data to yt" in {
    doTheTest(dataSize = 1000000)
  }

  it should "check if the inconsistent_dynamic_write is explicitly set to true" in {
    an [InconsistentDynamicWriteException] should be thrownBy {
      doTheTest(setInconsistentDynamicWrite = false)
    }
  }

  it should "check that the batch size is less than or equal 50K" in {
    a [TooLargeBatchException] should be thrownBy {
      doTheTest(dynBatchSize = Some(200000))
    }
  }

  it should "check that the table is mounted" in {
    a [TableNotMountedException] should be thrownBy {
      doTheTest(mountTable = false)
    }
  }

  it should "not write to the table with not matching schema" in {
    val sparkException = the [SparkException] thrownBy {
      val tableSchema = TableSchema.builder()
          .addValue("index", ColumnValueType.INT64)
          .addValue("content", ColumnValueType.STRING)
          .build()
      doTheTest(externalTableSchema = Some(tableSchema))
    }

    sparkException.getMessage shouldEqual "Job aborted."
  }

  it should "write the data to the table when dataframe contains some part of the table's columns" in {
    doTheTest(strictRowCheck = false, columnShift = 1, schemaModifier = { baseSchema =>
      TableSchema.builder()
          .addValue("extra_1", ColumnValueType.STRING)
          .addAll(baseSchema.getColumns)
          .addValue("extra_2", ColumnValueType.INT64)
          .addValue("extra_3", ColumnValueType.DOUBLE)
          .build()
    })
  }

  it should "not write the data to the table when dataframe does not contain all key columns" in {
    val sparkException = the [SparkException] thrownBy {
        doTheTest(schemaModifier = { baseSchema =>
            TableSchema.builder()
                .addKey("extra_key", ColumnValueType.INT64)
                .addAll(baseSchema.getColumns)
                .addValue("extra_value", ColumnValueType.STRING)
                .build()
        })
    }

    sparkException.getMessage shouldEqual "Job aborted."
  }

  it should "check that the saveMode is set to Append" in {
    an [AnalysisException] should be thrownBy {
      doTheTest(saveMode = SaveMode.ErrorIfExists)
    }
  }

  it should "write to a static table if it doesn't exist at specified path" in {
    doTheTest(createNewTable = false, mountTable = false)
  }

  private def doTheTest(dataSize: Int = 10,
                        dynBatchSize: Option[Int] = None,
                        setInconsistentDynamicWrite: Boolean = true,
                        createNewTable: Boolean = true,
                        mountTable: Boolean = true,
                        externalTableSchema: Option[TableSchema] = None,
                        strictRowCheck: Boolean = true,
                        columnShift: Int = 0,
                        saveMode: SaveMode = SaveMode.Append,
                        schemaModifier: TableSchema => TableSchema = identity
                       ): Unit = {
    val sampleData = generateSampleData(dataSize)

    def writeDataLambda(): Unit = {
      import spark.implicits._

      val df = spark.createDataset(sampleData)

      if (createNewTable) {
        val tableSchema = if (externalTableSchema.isEmpty) {
          schemaModifier(SchemaConverter.tableSchema(df.schema, Unordered, Map.empty))
        } else {
          externalTableSchema.get
        }
        YtWrapper.createDynTable(tmpPath, tableSchema)
      }

      if (mountTable) {
        YtWrapper.mountTableSync(tmpPath, 1.minute)
      }

      try {
        var dfWriter = df.write
          .format("yt")
          .mode(saveMode)

        if (setInconsistentDynamicWrite) {
          dfWriter = dfWriter.option("inconsistent_dynamic_write", "true")
        }

        dfWriter.save("ytTable:/" + tmpPath)
      } finally {
        if (mountTable) {
          YtWrapper.unmountTableSync(tmpPath, 1.minute)
        }
      }
    }

    dynBatchSize match {
      case Some(dynBatchSizeVal) => withConf("spark.yt.write.dynBatchSize", dynBatchSizeVal.toString)(writeDataLambda())
      case None => writeDataLambda()
    }

    val yPath = YPath.simple(YtWrapper.formatPath(tmpPath))

    val outputPathAttributes = YtWrapper.attributes(yPath, None, Set.empty[String])
    outputPathAttributes("dynamic").boolValue() shouldBe createNewTable

    YtDataCheck.yPathShouldContainExpectedData(yPath, sampleData, strictRowCheck, columnShift)(_.getValues.get(0 + columnShift).stringValue())
  }
}

object YtDynamicTableWriterTest {
  case class SampleRow(key: String,
                       value: String,
                       score: Int,
                       square: Long,
                       cube: Long,
                       root: Double,
                       flag: Boolean,
                       //TODO: deal with binary types - blob: Array[Byte],
                       //TODO: deal with Timestamp type - ts: Timestamp
  )

  implicit val sampleRowOrdering: Ordering[SampleRow] = Ordering.by(_.key)

  def generateSampleData(limit: Int): Seq[SampleRow] = {
    (1 to limit).map { n =>
      val key = s"key_${Integer.toString(n, 36)}"
      SampleRow(key,
        "abracadabra",
        n,
        n.longValue() * n,
        n.longValue() * n * n,
        Math.sqrt(n),
        n % 2 == 0
        //TODO: deal with binary types - MD5Hash.digest(key).getDigest
        //TODO: deal with Timestamp type - new Timestamp(System.currentTimeMillis())
      )
    }
  }
}
