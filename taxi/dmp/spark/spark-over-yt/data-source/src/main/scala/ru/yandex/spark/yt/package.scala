package ru.yandex.spark

import org.apache.spark.sql.types._
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

package object yt {
  implicit class YtReader(reader: DataFrameReader) {
    def partitions(partitions: Int): DataFrameReader = {
      reader.option(DefaultSourceParameters.partitionsParam, partitions)
    }

    def proxy(proxy: String): DataFrameReader = {
      reader.option(DefaultSourceParameters.proxyParam, proxy)
    }

    def hume: DataFrameReader = proxy("hume")

    def yt(path: String): DataFrame = reader.format("ru.yandex.spark.yt").load(path)

    def schemaHint(schemaHint: StructType): DataFrameReader = {
      reader.option(DefaultSourceParameters.isSchemaFullParam, false).schema(schemaHint)
    }

    def schemaHint(structField: StructField, structFields: StructField*): DataFrameReader = {
      schemaHint(StructType(structField +: structFields))
    }

    def schemaHint(field: (String, DataType), fields: (String, DataType)*): DataFrameReader = {
      schemaHint(StructField(field._1, field._2), fields.map(f => StructField(f._1, f._2)):_*)
    }

    def schemaHint2(field: (String, DataType), fields: (String, DataType)*): DataFrameReader = {
      (field +: fields).foldLeft(reader){case (result, (name, dataType)) =>
          result.option(s"${name}_hint", SchemaConverter.stringType(dataType))
      }
    }
  }

  implicit class YtWriter[T](writer: DataFrameWriter[T]) {
    def yt(path: String): Unit = writer.format("ru.yandex.spark.yt").save(path)

    def proxy(proxy: String): DataFrameWriter[T] = {
      writer.option(DefaultSourceParameters.proxyParam, proxy)
    }

    def hume: DataFrameWriter[T] = proxy("hume")

    def optimizeFor(optimizeMode: OptimizeMode): DataFrameWriter[T] = {
      writer.option("optimize_for", optimizeMode.name)
    }
  }

  implicit class YtDataFrame(df: DataFrame) {
    def selectAs[T <: Product : TypeTag]: Dataset[T] = {
      import df.sparkSession.implicits._
      val fields = Encoders.product[T].schema.fieldNames
      df.select(fields.head, fields.tail:_*).as[T]
    }
  }
}
