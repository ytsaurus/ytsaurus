package org.apache.spark

import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.SparkConf")
class SparkConfDecorators {

  @DecoratedMethod
  private[spark] def loadFromSystemProperties(silent: Boolean): SparkConf = {
    val self = __loadFromSystemProperties(silent)
    SparkConfExtensions.loadFromEnvironment(self, silent)
    self
  }

  private[spark] def __loadFromSystemProperties(silent: Boolean): SparkConf = ???
}

private[spark] object SparkConfExtensions {
  private[spark] def loadFromEnvironment(conf: SparkConf, silent: Boolean): SparkConf = {
    for ((key, value) <- sys.env if key.startsWith("SPARK_")) {
      conf.set(SparkConfExtensions.envToConfName(key), value, silent)
    }
    conf
  }

  private[spark] def envToConfName(envName: String): String = {
    envName.toLowerCase().replace("_", ".")
  }

  private[spark] def confToEnvName(confName: String): String = {
    confName.replace(".", "_").toUpperCase()
  }
}
