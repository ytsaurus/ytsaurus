package org.apache.spark.deploy.worker

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

import java.net.URI

@Decorate
@OriginClass("org.apache.spark.deploy.worker.DriverWrapper")
object DriverWrapperDecorators {

  @DecoratedMethod
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val hadoopConf = SparkHadoopUtil.newConfiguration(conf)
    try {
      __main(args)
    } finally {
      FileSystem.get(new URI("yt:///"), hadoopConf).close()
    }
  }

  def __main(args: Array[String]): Unit = ???

}
