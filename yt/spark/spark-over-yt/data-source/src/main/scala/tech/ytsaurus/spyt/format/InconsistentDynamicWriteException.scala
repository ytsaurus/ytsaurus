package tech.ytsaurus.spyt.format

case class InconsistentDynamicWriteException(message: String = null, cause: Throwable = null)
    extends Exception(message, cause)
