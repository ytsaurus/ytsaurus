package tech.ytsaurus.spyt.format

case class TooLargeBatchException(message: String = null, cause: Throwable = null)
  extends Exception(message, cause)
