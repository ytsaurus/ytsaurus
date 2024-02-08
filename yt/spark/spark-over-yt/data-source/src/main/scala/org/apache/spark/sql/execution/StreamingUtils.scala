package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType

object StreamingUtils {
  def createStreamingDataFrame(sqlContext: SQLContext, rdd: RDD[InternalRow], schema: StructType): DataFrame = {
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }
}
