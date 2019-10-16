package ru.yandex.spark

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import ru.yandex.spark.yt.format.{GlobalTableOptions, YtSourceStrategy}
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.spark.yt.utils.DefaultRpcCredentials

package object yt {
  SparkSession.getActiveSession.foreach(setup)

  def setup(spark: SparkSession): SparkSession = {
    import ru.yandex.spark.yt.format.SparkYtOptions._
    spark.sqlContext.setConf("fs.yt.impl", "ru.yandex.spark.yt.format.YtFileSystem")
    spark.sqlContext.setConf("fs.defaultFS", "yt:///")
    spark.sqlContext.setYtConf("user", DefaultRpcCredentials.user)
    spark.sqlContext.setYtConf("token", DefaultRpcCredentials.token)
    spark
  }

  def restartSparkWithExtensions(): SparkSession = {
    restartSparkWithExtensions(SparkSession.getActiveSession.get)
  }

  def restartSparkWithExtensions(spark: SparkSession): SparkSession = {
    val conf = spark.conf
    spark.stop()
    startSparkWithExtensions(conf)
  }

  def startSparkWithExtensions(conf: RuntimeConfig): SparkSession = {
    val newSession = SparkSession.builder()
      .config(new SparkConf().setAll(conf.getAll))
      .withExtensions(_.injectPlannerStrategy(_ => YtSourceStrategy))
      .getOrCreate()
    setup(newSession)
  }

  implicit class YtReader(reader: DataFrameReader) {
    def yt(path: String): DataFrame = reader.format("yt").load(path)

    def yt(path: String, filesCount: Int): DataFrame = {
      GlobalTableOptions.setFilesCount(path, filesCount)
      yt(path)
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
    def yt(path: String): Unit = writer.format("yt").save(path)

    def optimizeFor(optimizeMode: OptimizeMode): DataFrameWriter[T] = {
      writer.option("optimize_for", optimizeMode.name)
    }
  }

  implicit class RichLogger(log: Logger) {
    def debugLazy(message: => String): Unit = {
      if (log.isDebugEnabled) {
        log.debug(message)
      }
    }
  }

}
