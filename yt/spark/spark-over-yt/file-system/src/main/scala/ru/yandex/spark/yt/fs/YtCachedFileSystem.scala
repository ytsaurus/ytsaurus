package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.slf4j.LoggerFactory

import java.io._
import java.net.URI
import java.nio.file.{Files, StandardCopyOption}
import java.util.UUID
import scala.language.postfixOps

@SerialVersionUID(1L)
class YtCachedFileSystem extends YtFileSystem {
  private val log = LoggerFactory.getLogger(getClass)

  private val DIRECTORY_FOR_CACHED_FILES = "cachedDownloadedFiles"
  private var cachedDirPath: java.nio.file.Path = _

  override def initialize(uri: URI, conf: Configuration): Unit = {
    super.initialize(uri, conf)
    cachedDirPath = java.nio.file.Path.of(
      Option(System.getenv("SPARK_HOME")).getOrElse(System.getProperty("java.io.tmpdir")),
      DIRECTORY_FOR_CACHED_FILES)
    log.info(s"Path to cached files: $cachedDirPath")
  }

  private def getCachedFile(f: Path): File = {
    val filename = f.toUri.getPath.replace('/', '+')
    val localPath = cachedDirPath.resolve(filename)
    localPath.toFile
  }

  private def copyStream(in: InputStream, out: OutputStream, closeStreams: Boolean = true): Unit = {
    try {
      val buf = new Array[Byte](8192)
      var n = 0
      while (n != -1) {
        n = in.read(buf)
        if (n != -1) {
          out.write(buf, 0, n)
        }
      }
    } finally {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
  }

  private def moveFile(in: File, out: File): Unit = {
    Files.move(
      java.nio.file.Path.of(in.getPath),
      java.nio.file.Path.of(out.getPath),
      StandardCopyOption.REPLACE_EXISTING
    )
  }

  override def open(f: Path, bufferSize: Int): FSDataInputStream = convertExceptions {
    log.info(s"Cached opening file ${f.toUri.toString}")
    statistics.incrementReadOps(1)
    val cachedFile = getCachedFile(f)
    if (!cachedFile.exists()) {
      Files.createDirectories(cachedDirPath)
      val tempFile = new File(cachedDirPath.toFile, s"caching-${UUID.randomUUID()}.tmp")
      try {
        log.info(s"Fetching ${f.toUri.toString} to $tempFile")
        val in = super.open(f, bufferSize)
        val out = new FileOutputStream(tempFile)
        copyStream(in, out)
        if (!tempFile.exists()) {
          throw new IOException(s"Cached downloading ${f.toUri.toString} to $tempFile failed")
        }

        log.info(s"Copying $tempFile to $cachedFile")
        moveFile(tempFile, cachedFile)
        if (!cachedFile.exists()) {
          throw new IOException(s"Copying temp file from $tempFile to $cachedFile failed")
        }
      } finally {
        if (tempFile.exists()) {
          val deleteResult = tempFile.delete()
          log.info(s"Delete temp file $tempFile status: $deleteResult")
        }
      }
    } else {
      log.info("Reusing cached version")
    }

    new FSDataInputStream(new LocalFSInputStream(cachedFile))
  }
}
