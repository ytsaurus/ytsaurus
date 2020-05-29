package ru.yandex.spark

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import ru.yandex.spark.yt.format.conf.YtTableSparkSettings
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.fs.{GlobalTableSettings, YtClientProvider}
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.yt.ytclient.proxy.YtClient
import org.apache.spark.sql.functions._

package object yt {
  lazy val yt: YtClient = YtClientProvider.ytClient(ytClientConfiguration(SparkSession.getDefaultSession.get))

  private def normalizePath(path: String): String = {
    if (path.startsWith("//")) path.drop(1) else path
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

  implicit class YtDataset[T](df: Dataset[T]) {
    def withYsonColumn(name: String, column: Column): DataFrame = {
      val colSchema = df.withColumn(name, column).schema(name)
      val metadata = new MetadataBuilder()
        .withMetadata(colSchema.metadata)
        .putBoolean("skipNulls", true)
        .build()
      val newColumn = column.as(name, metadata)
      df.withColumn(name, newColumn)
    }

    def selectAs[S](implicit encoder: Encoder[S]): Dataset[S] = {
      val names = encoder.schema.fieldNames
      df.select(names.head, names.tail:_*).as[S]
    }
  }
}
