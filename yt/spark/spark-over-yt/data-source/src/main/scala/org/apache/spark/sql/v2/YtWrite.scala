package org.apache.spark.sql.v2

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.v2.FileWrite
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import tech.ytsaurus.spyt.format.YtOutputWriterFactory
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration

case class YtWrite(paths: Seq[String],
                   formatName: String,
                   supportsDataType: DataType => Boolean,
                   info: LogicalWriteInfo)
  extends FileWrite with Logging {

  override def prepareWrite(sqlConf: SQLConf,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    YtOutputWriterFactory.create(
        SparkYtWriteConfiguration(sqlConf),
        ytClientConfiguration(sqlConf),
        options,
        dataSchema,
        job.getConfiguration
    )
  }
}

