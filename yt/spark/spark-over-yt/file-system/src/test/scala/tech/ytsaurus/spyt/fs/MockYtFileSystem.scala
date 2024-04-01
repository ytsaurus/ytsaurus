package tech.ytsaurus.spyt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSInputStream, FileStatus, Path}
import tech.ytsaurus.client.CompoundClient

import java.net.URI

class MockYtFileSystem extends YtFileSystem {
  override protected lazy val yt: CompoundClient = null

  override def initialize(uri: URI, conf: Configuration): Unit = ()

  override def getFileStatus(f: Path): FileStatus = {
    new FileStatus(0, false, 0, 0, 0, f)
  }

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    new FSDataInputStream(new MockFSInputStream())
  }

  override def getUri: URI = URI.create("")
}

class MockFSInputStream extends FSInputStream {

  override def seek(pos: Long): Unit = ()

  override def getPos: Long = 0L

  override def seekToNewSource(targetPos: Long): Boolean = false

  override def read(): Int = -1
}
