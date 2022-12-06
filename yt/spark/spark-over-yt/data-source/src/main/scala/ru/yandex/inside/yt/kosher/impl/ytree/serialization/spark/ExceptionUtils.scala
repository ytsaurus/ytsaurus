package ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark

import java.io.{FileNotFoundException, IOException}
import java.util.concurrent.{ExecutionException, TimeoutException}

object ExceptionUtils {
  class RuntimeIoException(cause: Throwable) extends RuntimeException(cause)
  class FileNotFoundIoException(cause: Throwable) extends RuntimeException(cause)
  class InterruptedRuntimeException(cause: Throwable) extends RuntimeException(cause)
  class ExecutionRuntimeException(cause: Throwable) extends RuntimeException(cause)
  class TimeoutRuntimeException(cause: Throwable) extends RuntimeException(cause)

  // Copied from ru.yandex.misc.ExceptionUtils.translate
  def translate(e: Exception): RuntimeException = e match {
    case exception: RuntimeException => exception
    case _: FileNotFoundException => new FileNotFoundIoException(e)
    case _: IOException => new RuntimeIoException(e)
    case _: InterruptedException => new InterruptedRuntimeException(e)
    case _: ExecutionException => new ExecutionRuntimeException(e.getCause)
    case _: TimeoutException => new TimeoutRuntimeException(e)
    case _ => new RuntimeException(e)
  }
}
