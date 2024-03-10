package org.apache.spark.sql

import tech.ytsaurus.spyt.format.optimizer.YtSortedTableMarkerRule
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.SparkSession$Builder")
class SparkSessionBuilderDecorators {

  @DecoratedMethod
  def getOrCreate(): SparkSession = {
    val spark = __getOrCreate()
    spark.experimental.extraOptimizations ++= Seq(new YtSortedTableMarkerRule(spark))
    spark
  }

  def __getOrCreate(): SparkSession = ???
}
