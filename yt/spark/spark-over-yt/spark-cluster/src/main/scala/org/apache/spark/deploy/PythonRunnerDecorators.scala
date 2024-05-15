package org.apache.spark.deploy

import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.deploy.PythonRunner")
object PythonRunnerDecorators {

  @DecoratedMethod
  def main(args: Array[String]): Unit = {
    val redirectToStderr = System.getProperty("spark.ytsaurus.redirectToStderr", "false").toBoolean
    if (redirectToStderr) {
      System.setOut(System.err)
    }
    __main(args)
  }

  def __main(args: Array[String]): Unit = ???
}
