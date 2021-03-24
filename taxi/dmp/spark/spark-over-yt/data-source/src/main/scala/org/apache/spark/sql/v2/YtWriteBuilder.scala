package org.apache.spark.sql.v2

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.v2.FileWriteBuilder
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import ru.yandex.spark.yt.format.conf.{SparkYtWriteConfiguration, YtTableSparkSettings}
import ru.yandex.spark.yt.format.{YtOutputCommitter, YtOutputWriter}
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.serializers.SchemaConverter

class YtWriteBuilder(paths: Seq[String],
                     formatName: String,
                     supportsDataType: DataType => Boolean,
                     info: LogicalWriteInfo)
  extends FileWriteBuilder(paths, formatName, supportsDataType, info) with Logging {

  override def prepareWrite(sqlConf: SQLConf,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    SchemaConverter.checkSchema(dataSchema)

    val ytClientConf = ytClientConfiguration(sqlConf)
    val writeConfiguration = SparkYtWriteConfiguration(sqlConf)
    YtTableSparkSettings.serialize(options, dataSchema, job.getConfiguration)

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = ""

      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        val transaction = YtOutputCommitter.getWriteTransaction(context.getConfiguration)
        new YtOutputWriter(path, dataSchema, ytClientConf, writeConfiguration, transaction, options)
      }
    }
  }
}

