package tech.ytsaurus.spyt

package object exceptions {
  case class InconsistentDynamicWriteException(message: String = null, cause: Throwable = null)
    extends IllegalArgumentException(message, cause)

  case class TooLargeBatchException(message: String = null, cause: Throwable = null)
    extends IllegalArgumentException(message, cause)

  case class TableNotMountedException(message: String = null, cause: Throwable = null)
    extends IllegalStateException(message, cause)
}
