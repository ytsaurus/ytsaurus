package ru.yandex.spark.yt.arrow.test

import java.io.{BufferedOutputStream, FileOutputStream}
import java.util.UUID

import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.yt.wrapper.YtWrapper

import scala.concurrent.duration._
import scala.language.postfixOps

object ArrowTest extends App {
  private val log = Logger.getLogger(getClass)
  val parsedArgs = Args(args)

  val path = parsedArgs.optional("path").getOrElse("//home/sashbel/data/eventlog_city_small")
  val outputPath = parsedArgs.optional("output").getOrElse(s"/tmp/arrow-${UUID.randomUUID()}")
  val proxy = parsedArgs.optional("proxy").getOrElse("man2-4193-e31.hume.yt.gencfg-c.yandex.net:27002")
  val timeout = parsedArgs.optional("timeout").map(_.toInt.seconds).getOrElse(5 minutes)
  val bufferSize = parsedArgs.optional("buffer").map(_.toInt).getOrElse(65536)
  val logFrequency = parsedArgs.optional("log-frequency").map(_.toLong).getOrElse(100L * 1024 * 1024)

  val client = YtWrapper.createYtClient(proxy, 5 minutes)
  try {
    val stream = YtWrapper.readTableArrowStream(path, 1 minute)(client.yt)
    try {
      val out = new BufferedOutputStream(new FileOutputStream(outputPath), bufferSize)
      try {
        val buffer = new Array[Byte](bufferSize)
        var sum = 0L
        Stream.continually(stream.read(buffer, 0, bufferSize)).takeWhile(_ > 0).foreach { len =>
          if ((sum % logFrequency) < bufferSize) {
            log.info(s"Read $sum bytes")
          }
          out.write(buffer, 0, len)
          sum += len
        }
        log.info(s"Read $sum bytes")
      } finally {
        out.close()
      }
    } finally {
      stream.close()
    }
  } finally {
    client.close()
  }
}
