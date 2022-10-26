package ru.yandex.spark.launcher.rest

import java.net.BindException

import io.netty.channel.unix.Errors.NativeIoException
import org.eclipse.jetty.util.MultiException
import org.slf4j.LoggerFactory

object Utils {
  private val log = LoggerFactory.getLogger(getClass)

  def isBindCollision(exception: Throwable): Boolean = {
    import scala.collection.JavaConverters._
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: MultiException =>
        e.getThrowables.asScala.exists(isBindCollision)
      case e: NativeIoException =>
        (e.getMessage != null && e.getMessage.startsWith("bind() failed: ")) ||
          isBindCollision(e.getCause)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  def startServiceOnPort[T](startPort: Int,
                            startService: Int => (T, Int),
                            maxRetries: Int,
                            serviceName: String = ""): (T, Int) = {

    require(1024 <= startPort && startPort < 65536, "startPort should be between 1024 and 65535 (inclusive)")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = startPort + offset
      try {
        val (service, port) = startService(tryPort)
        log.info(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage = if (startPort == 0) {
              s"${e.getMessage}: Service$serviceString failed after " +
                s"$maxRetries retries (on a random free port)! " +
                s"Consider explicitly setting the appropriate binding address for " +
                s"the service$serviceString (for example spark.driver.bindAddress " +
                s"for SparkDriver) to the correct binding address."
            } else {
              s"${e.getMessage}: Service$serviceString failed after " +
                s"$maxRetries retries (starting from $startPort)! Consider explicitly setting " +
                s"the appropriate port for the service$serviceString (for example spark.ui.port " +
                s"for SparkUI) to an available port or increasing spark.port.maxRetries."
            }
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          log.info(s"Service$serviceString could not bind on port $tryPort. " +
              s"Attempting port ${tryPort + 1}.")
      }
    }
    // Should never happen
    throw new RuntimeException(s"Failed to start service$serviceString on port $startPort")
  }
}
