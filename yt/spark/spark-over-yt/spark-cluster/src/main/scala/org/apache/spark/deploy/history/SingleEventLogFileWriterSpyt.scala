package org.apache.spark.deploy.history

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import tech.ytsaurus.spyt.patch.annotations.{OriginClass, Subclass}

import java.net.URI

@Subclass
@OriginClass("org.apache.spark.deploy.history.SingleEventLogFileWriter")
class SingleEventLogFileWriterSpyt(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends SingleEventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf) {

  override def stop(): Unit = {
    super.stop()
    fileSystem.close()
  }
}
