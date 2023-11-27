package tech.ytsaurus.spyt.format

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

class HivePartitioningTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers with TestUtils {

  override def beforeEach(): Unit = {
    super.beforeEach()
    createHivePartitionedTable()
  }

  private val ytSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("id", ColumnValueType.INT64)
    .addValue("name", ColumnValueType.STRING)
    .build()

  private val sparkSchema = StructType(Seq(
    StructField("id", LongType),
    StructField("name", StringType)
  ))

  private val partitionedSparkSchema = sparkSchema
    .add(StructField("external", IntegerType))
    .add(StructField("internal", IntegerType))

  "spark.read.yt" should "read partitioned data with recursiveFileLookup option set to false" in {
    import tech.ytsaurus.spyt._
    import spark.implicits._

    val df = spark.read.option("recursiveFileLookup", "false").yt(tmpPath)

    df.schema.toDDL shouldEqual partitionedSparkSchema.toDDL

    val sample = df.where($"external" === 5 and $"internal" === 2).select($"id").as[Long].collect()

    sample.length shouldBe 10
    sample should contain allElementsOf (520L to 529L)

    df.count() shouldBe 500L
  }

  it should "read partitioned data with recursiveFileLookup option set to true" in {
    import tech.ytsaurus.spyt._
    val df = spark.read.option("recursiveFileLookup", "true").yt(tmpPath)

    df.count() shouldBe 500L
    df.schema.toDDL shouldEqual sparkSchema.toDDL
  }

  it should "read hive partitioned data with basePath" in {
    import tech.ytsaurus.spyt._
    val df = spark.read.option("recursiveFileLookup", "false")
      .option("basePath", "ytTable:/" + tmpPath).yt(tmpPath + "/external=1")

    df.count() shouldBe 50L
    df.schema.toDDL shouldEqual partitionedSparkSchema.toDDL
  }

  private def createHivePartitionedTable(): Unit = {
    YtWrapper.createDir(tmpPath)
    (1 to 10).foreach { external =>
      val pathPrefix = s"$tmpPath/external=$external"
      YtWrapper.createDir(pathPrefix)
      (1 to 5).foreach { internal =>
        val idPrefix = external*100 + internal*10
        val rows = (0 to 9).map { idSuffix =>
          val id = idPrefix + idSuffix
          s"""{id = $id; name = "Name for $id"}"""
        }
        val path = s"$pathPrefix/internal=$internal"
        writeTableFromYson(rows, path, ytSchema)
      }
    }
  }
}
