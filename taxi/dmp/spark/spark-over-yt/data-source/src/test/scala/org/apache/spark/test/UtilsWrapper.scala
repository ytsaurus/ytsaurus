package org.apache.spark.test

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

object UtilsWrapper {
  def classForName[C](name: String): Class[C] = Utils.classForName(name)

  def getConf[T](spark: SparkSession, conf: ConfigEntry[T]): T = {
    spark.sparkContext.getConf.get(conf)
  }
}
