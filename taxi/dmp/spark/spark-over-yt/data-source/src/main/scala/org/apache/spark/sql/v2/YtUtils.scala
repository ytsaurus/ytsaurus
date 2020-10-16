package org.apache.spark.sql.v2

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.yson.YsonType
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.fs.{YtClientProvider, YtPath}
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.YtClient

object YtUtils {
  def inferSchema(sparkSession: SparkSession,
                  parameters: Map[String, String],
                  files: Seq[FileStatus]): Option[StructType] = {
    files.headOption.map { fileStatus =>
      val schemaHint = SchemaConverter.schemaHint(parameters)
      implicit val client: YtClient = YtClientProvider.ytClient(ytClientConfiguration(sparkSession))
      val path = fileStatus.getPath match {
        case ytPath: YtPath => ytPath.stringPath
        case p => YtPath.basePath(p)
      }
      val schemaTree = YtWrapper.attribute(path, "schema")
      val schema = SchemaConverter.sparkSchema(schemaTree, schemaHint)

      // backward compatibility
      if (sparkSession.conf.getOption("spark.yt.cluster.version").exists(_ < "3.0.1-1.1.1+yandex")) {
        StructType(schema.map {
          case f if f.dataType == YsonType => f.copy(dataType = BinaryType)
          case f => f
        })
      } else schema
    }
  }

}
