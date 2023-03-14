package tech.ytsaurus.spyt.wrapper.client

import java.nio.file.{Files, Path, Paths}
import tech.ytsaurus.client.rpc.YTsaurusClientAuth

object DefaultRpcCredentials {
  def token: String = {
    sys.env.getOrElse("YT_TOKEN", readFileFromHome(".yt", "token"))
  }

  def user: String = sys.env.getOrElse("YT_USER", System.getProperty("user.name"))

  def credentials: YTsaurusClientAuth = {
    YTsaurusClientAuth.builder().setUser(user).setToken(token).build()
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
