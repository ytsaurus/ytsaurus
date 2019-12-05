package org.apache.spark.deploy.master

import org.apache.log4j.Logger
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer}
import org.apache.spark.util.Utils
import ru.yandex.spark.yt.utils.YtTableUtils
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.reflect.ClassTag

class YtPersistenceEngine(baseDir: String,
                          serializer: Serializer)
                         (implicit yt: YtClient) extends PersistenceEngine {
  private val log = Logger.getLogger(getClass)

  YtTableUtils.createDir(baseDir, ignoreExisting = true)

  override def persist(name: String, obj: Object): Unit = {
    log.info(s"Persist object $name")
    serializeIntoFile(s"$baseDir/$name", obj)
  }

  override def unpersist(name: String): Unit = {
    log.info(s"Unpersist object $name")
    YtTableUtils.removeIfExists(s"$baseDir/$name")
  }

  override def read[T](prefix: String)(implicit evidence$1: ClassTag[T]): Seq[T] = {
    log.info(s"Read prefix $prefix")
    val paths = YtTableUtils.listDirectory(baseDir).filter(_.startsWith(prefix))
    paths.map(deserializeFromFile(_))
  }

  private def serializeIntoFile(path: String, value: AnyRef) {
    log.info(s"Create file $path")
    YtTableUtils.createFile(path)
    log.info(s"Write to file $path")
    val fileOut = YtTableUtils.writeToFile(path, java.time.Duration.ofMinutes(5))
    var out: SerializationStream = null
    Utils.tryWithSafeFinally {
      out = serializer.newInstance().serializeStream(fileOut)
      out.writeObject(value)
    } {
      fileOut.close()
      if (out != null) {
        out.close()
      }
    }
  }

  private def deserializeFromFile[T](path: String)(implicit m: ClassTag[T]): T = {
    log.info(s"Deserialize file $path")
    val fileIn = YtTableUtils.readFile(path)
    var in: DeserializationStream = null
    try {
      in = serializer.newInstance().deserializeStream(fileIn)
      in.readObject[T]()
    } finally {
      fileIn.close()
      if (in != null) {
        in.close()
      }
    }
  }
}
