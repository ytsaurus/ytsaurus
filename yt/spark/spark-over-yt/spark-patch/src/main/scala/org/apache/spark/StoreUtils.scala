package org.apache.spark

import org.apache.spark.status.AppStatusStore

object StoreUtils {
  def getStatusStore(sc: SparkContext): AppStatusStore = sc.statusStore
}
