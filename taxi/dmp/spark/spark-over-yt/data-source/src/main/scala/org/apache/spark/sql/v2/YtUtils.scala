package org.apache.spark.sql.v2

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.yson.YsonType
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.fs.YtPath
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientProvider
import ru.yandex.yt.ytclient.proxy.CompoundClient

import scala.collection.mutable

object YtUtils {
  private def getFilePath(fileStatus: FileStatus): String = {
    fileStatus.getPath match {
      case ytPath: YtPath => ytPath.stringPath
      case p => YtPath.basePath(p)
    }
  }

  private def getOldFormatSchema(schema: StructType): StructType = {
    StructType(schema.map {
      case f if f.dataType == YsonType => f.copy(dataType = BinaryType)
      case f => f
    })
  }

  private def getCompatibleSchema(sparkSession: SparkSession, schema: StructType): StructType = {
    // backward compatibility
    if (sparkSession.conf.getOption("spark.yt.cluster.version").exists(_ < "3.0.1-1.1.1+yandex")) {
      getOldFormatSchema(schema)
    } else {
      schema
    }
  }

  def inferSchema(sparkSession: SparkSession,
                  parameters: Map[String, String],
                  files: Seq[FileStatus]): Option[StructType] = {
    implicit val client: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(sparkSession))
    val (_, schema) = files.foldLeft((Set.empty[String], Option.empty[StructType])) {
      case ((curSet, curSchema), fileStatus) =>
        val pathString = fileStatus.getPath.toString
        if (curSet.contains(pathString)) {
          (curSet, curSchema)
        } else {
          val schemaHint = SchemaConverter.schemaHint(parameters)
          val path = getFilePath(fileStatus)
          val schemaTree = YtWrapper.attribute(path, "schema")
          val sparkSchema = SchemaConverter.sparkSchema(schemaTree, schemaHint)
          val compatibleSchema = getCompatibleSchema(sparkSession, sparkSchema)
          (curSet + pathString,
            curSchema.map(_.merge(compatibleSchema)).orElse(Some(compatibleSchema)))
        }
    }
    schema
  }
}
