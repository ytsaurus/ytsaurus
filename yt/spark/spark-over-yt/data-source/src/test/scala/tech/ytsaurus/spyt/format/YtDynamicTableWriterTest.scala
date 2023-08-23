package tech.ytsaurus.spyt.format

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.MD5Hash
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.request.{ModifyRowsRequest, TransactionalOptions, WriteSerializationContext, WriteTable}
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.fs.conf._
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.{SortColumns, UniqueKeys, WriteSchemaHint, WriteTypeV3}
import tech.ytsaurus.spyt.serializers.{InternalRowSerializer, SchemaConverter}
import tech.ytsaurus.spyt.serializers.SchemaConverter.{Sorted, Unordered}
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import java.sql.Timestamp
import java.util
import java.util.Date
import scala.concurrent.duration._
import scala.language.postfixOps

class YtDynamicTableWriterTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers {
  import YtDynamicTableWriterTest._

  override val numExecutors = 8

  "YtDynamicTableWriter" should "write lots of data to yt" in {
    doTheTest()
  }

  it should "check if the inconsistent_dynamic_write is explicitly set to true" in {
    an [InconsistentDynamicWriteException] should be thrownBy {
      doTheTest(dataSize = 10, setInconsistentDynamicWrite = false)
    }
  }

  it should "check that the batch size is less than or equal 50K" in {
    a [TooLargeBatchException] should be thrownBy {
      doTheTest(dataSize = 10, dynBatchSize = Some(200000))
    }
  }

  private def doTheTest(dataSize: Int = 1000000, dynBatchSize: Option[Int] = None, setInconsistentDynamicWrite: Boolean = true): Unit = {
    val sampleData = generateSampleData(dataSize)

    def writeDataLambda(): Unit = {
      import spark.implicits._

      val df = spark.createDataset(sampleData)

      var dfWriter = df.write
        .format("yt")
        .option("dynamic", "true")
        .option("sort_columns", """["key"]""")
        .option("unique_keys", "true")

      if (setInconsistentDynamicWrite) {
        dfWriter = dfWriter.option("inconsistent_dynamic_write", "true")
      }

      dfWriter.save("yt:/" + tmpPath)
    }

    dynBatchSize match {
      case Some(dynBatchSizeVal) => withConf("spark.yt.write.dynBatchSize", dynBatchSizeVal.toString)(writeDataLambda())
      case None => writeDataLambda()
    }

    val yPath = YPath.simple(YtWrapper.formatPath(tmpPath))

    val outputPathAttributes = YtWrapper.attributes(yPath, None, Set.empty[String])
    outputPathAttributes("dynamic").boolValue() shouldBe true

    YtDataCheck.yPathShouldContainExpectedData(yPath, sampleData)(_.getValues.get(0).stringValue())
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
