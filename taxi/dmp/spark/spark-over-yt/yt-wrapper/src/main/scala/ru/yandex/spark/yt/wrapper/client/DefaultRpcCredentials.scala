package ru.yandex.spark.yt.wrapper.client

import java.nio.file.{Files, Paths}

import ru.yandex.yt.ytclient.rpc.RpcCredentials

object DefaultRpcCredentials {
  def token: String = {
    val tokenPath = Paths.get(System.getProperty("user.home"), ".yt", "token")
    val reader = Files.newBufferedReader(tokenPath)
    try {
      reader.readLine
    } finally {
      reader.close()
    }
  }

  def user: String = System.getProperty("user.name")

  def credentials: RpcCredentials = {
    new RpcCredentials(user, token)
  }
}
