package ru.yandex.spark.yt.submit

import java.io.{DataOutputStream, File, FileOutputStream}
import java.net.InetAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.security.SecureRandom

import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory

object PythonGatewayServer extends App {
  private val log = LoggerFactory.getLogger(getClass)

  private val secret = {
    val rnd = new SecureRandom()
    val secretBytes = new Array[Byte](256 / java.lang.Byte.SIZE)
    rnd.nextBytes(secretBytes)
    Hex.encodeHexString(secretBytes)
  }

  private val localhost = InetAddress.getLoopbackAddress()
  private val server = new py4j.GatewayServer.GatewayServerBuilder()
    .authToken(secret)
    .javaPort(0)
    .javaAddress(localhost)
    .callbackClient(py4j.GatewayServer.DEFAULT_PYTHON_PORT, localhost, secret)
    .build()

  server.start()

  val boundPort: Int = server.getListeningPort
  if (boundPort == -1) {
    log.error(s"PythonGatewayServer failed to bind; exiting")
    System.exit(1)
  } else {
    log.debug(s"Started PythonGatewayServer on port $boundPort")
  }

  val connectionInfoPath = new File(sys.env("_SPYT_SUBMIT_CONN_INFO_PATH"))
  val tmpPath = Files.createTempFile(connectionInfoPath.getParentFile().toPath(),
    "connection", ".info").toFile()

  val dos = new DataOutputStream(new FileOutputStream(tmpPath))
  dos.writeInt(boundPort)

  val secretBytes = secret.getBytes(UTF_8)
  dos.writeInt(secretBytes.length)
  dos.write(secretBytes, 0, secretBytes.length)
  dos.close()

  if (!tmpPath.renameTo(connectionInfoPath)) {
    log.error(s"Unable to write connection information to $connectionInfoPath.")
    System.exit(1)
  }

  // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
  while (System.in.read() != -1) {
    // Do nothing
  }
  log.debug("Exiting due to broken pipe from Python driver")
  System.exit(0)

}
