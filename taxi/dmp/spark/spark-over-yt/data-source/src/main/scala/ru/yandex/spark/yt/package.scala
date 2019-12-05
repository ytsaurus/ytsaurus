package ru.yandex.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import ru.yandex.spark.yt.conf.YtTableSparkSettings
import ru.yandex.spark.yt.format.{GlobalTableSettings, YtSourceStrategy}
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.spark.yt.utils.DefaultRpcCredentials
import ru.yandex.yt.ytclient.proxy.YtClient

package object yt {
  lazy val yt: YtClient = YtClientProvider.ytClient(YtClientConfigurationConverter(SparkSession.getDefaultSession.get))

  private def normalizePath(path: String): String = {
    if (path.startsWith("//")) path.drop(1) else path
  }

  def createSparkSession(conf: SparkConf = new SparkConf()): SparkSession = {
    SparkSession.builder()
      .config(conf)
      .withExtensions(_.injectPlannerStrategy(_ => new YtSourceStrategy))
      .getOrCreate()
  }

  def restartSparkWithExtensions(): SparkSession = {
    restartSparkWithExtensions(SparkSession.getActiveSession.get)
  }

  def restartSparkWithExtensions(spark: SparkSession): SparkSession = {
    val conf = spark.conf
    spark.stop()
    val sparkConf = new SparkConf()
      .setAll(conf.getAll)
      .set("spark.yt.user", DefaultRpcCredentials.user)
      .set("spark.yt.token", DefaultRpcCredentials.token)
    createSparkSession(sparkConf)
  }

  implicit class YtReader(reader: DataFrameReader) {
    def yt(paths: String*): DataFrame = reader.format("yt").load(paths.map(normalizePath): _*)

    def yt(path: String, filesCount: Int): DataFrame = {
      GlobalTableSettings.setFilesCount(normalizePath(path), filesCount)
      yt(normalizePath(path))
    }

    def schemaHint(schemaHint: StructType): DataFrameReader = {
      reader.options(SchemaConverter.serializeSchemaHint(schemaHint))
    }

    def schemaHint(structField: StructField, structFields: StructField*): DataFrameReader = {
      schemaHint(StructType(structField +: structFields))
    }

    def schemaHint(field: (String, DataType), fields: (String, DataType)*): DataFrameReader = {
      schemaHint(
        StructField(field._1, field._2),
        fields.map { case (name, dataType) => StructField(name, dataType) }: _*
      )
    }
  }

  implicit class YtWriter[T](writer: DataFrameWriter[T]) {
    def yt(path: String): Unit = writer.format("yt").save(normalizePath(path))

    def optimizeFor(optimizeMode: OptimizeMode): DataFrameWriter[T] = {
      writer.option(YtTableSparkSettings.OptimizeFor.name, optimizeMode.name)
    }

    def sortedBy(cols: String*): DataFrameWriter[T] = {
      writer.option(YtTableSparkSettings.SortColumns.name, cols.mkString(","))
    }
  }
}
