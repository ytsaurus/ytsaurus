package org.apache.spark.sql.internal

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.catalog.YTsaurusExternalCatalog
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.internal.SharedState$")
object SharedStateDecorators {
  @DecoratedMethod
  private[sql] def org$apache$spark$sql$internal$SharedState$$externalCatalogClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "in-memory" => classOf[YTsaurusExternalCatalog].getCanonicalName
      case _ => __org$apache$spark$sql$internal$SharedState$$externalCatalogClassName(conf)
    }
  }

  private[sql] def __org$apache$spark$sql$internal$SharedState$$externalCatalogClassName(conf: SparkConf): String = ???
}
