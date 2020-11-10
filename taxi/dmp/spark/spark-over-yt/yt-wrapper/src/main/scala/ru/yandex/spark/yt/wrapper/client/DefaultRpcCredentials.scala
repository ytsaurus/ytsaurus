package ru.yandex.spark.yt.wrapper.client

import java.nio.file.{Files, Path, Paths}

import ru.yandex.yt.ytclient.rpc.RpcCredentials

object DefaultRpcCredentials {
  def token: String = {
    sys.env.getOrElse("YT_TOKEN", readFileFromHome(".yt", "token"))
  }

  def user: String = sys.env.getOrElse("YT_USER", System.getProperty("user.name"))

  def credentials: RpcCredentials = {
    new RpcCredentials(user, token)
  }

  private def readFileFromHome(path: String*): String = {
    readFile(Paths.get(System.getProperty("user.home"), path: _*))
  }

  private def readFile(path: Path): String = {
    val reader = Files.newBufferedReader(path)
    try {
      reader.readLine
    } finally {
      reader.close()
    }
  }
}
