package ru.yandex.spark

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import ru.yandex.spark.yt.format.conf.YtTableSparkSettings
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.fs.conf.ConfigEntry
import ru.yandex.spark.yt.fs.{GlobalTableSettings, YtClientProvider}
import ru.yandex.spark.yt.serializers.{SchemaConverter, YtLogicalType}
import ru.yandex.spark.yt.wrapper.table.OptimizeMode
import ru.yandex.yt.ytclient.proxy.CompoundClient

package object yt {
  lazy val yt: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(SparkSession.getDefaultSession.get))

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

    def enableArrow(enable: Boolean): DataFrameReader = {
      reader.option(YtTableSparkSettings.ArrowEnabled.name, enable.toString)
    }

    def enableArrow: DataFrameReader = {
      enableArrow(true)
    }

    def disableArrow: DataFrameReader = {
      enableArrow(false)
    }
  }

  implicit class YtWriter[T](writer: DataFrameWriter[T]) {
    def yt(path: String): Unit = writer.format("yt").save(normalizePath(path))

    def option[S](config: ConfigEntry[S], value: S): DataFrameWriter[T] = {
      val stringValue = config.set(value)
      writer.option(config.name, stringValue)
    }

    def optimizeFor(optimizeMode: OptimizeMode): DataFrameWriter[T] = {
      writer.option(YtTableSparkSettings.OptimizeFor, optimizeMode.name)
    }

    def optimizeFor(optimizeMode: String): DataFrameWriter[T] = {
      optimizeFor(OptimizeMode.fromName(optimizeMode))
    }

    def sortedBy(cols: String*): DataFrameWriter[T] = {
      writer.option(YtTableSparkSettings.SortColumns, cols)
    }

    def schemaHint(schemaHint: Map[String, YtLogicalType]): DataFrameWriter[T] = {
      writer.option(YtTableSparkSettings.WriteSchemaHint, schemaHint)
    }

    def schemaHint(field: (String, YtLogicalType), fields: (String, YtLogicalType)*): DataFrameWriter[T] = {
      schemaHint(fields.toMap + field)
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
      df.select(names.head, names.tail: _*).as[S]
    }
  }

}
