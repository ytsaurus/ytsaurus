package ru.yandex.spark.yt

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import ru.yandex.yt.ytclient.rpc.RpcCredentials

case class DefaultSourceParameters(path: String,
                                   proxy: String,
                                   user: String,
                                   token: String,
                                   partitions: Option[Int],
                                   isSchemaFull: Boolean) {
  def rpcCredentials: RpcCredentials = new RpcCredentials(user, token)
}

object DefaultSourceParameters {
  val pathParam = "path"
  val proxyParam = "proxy"
  val userParam = "user"
  val tokenParam = "token"
  val partitionsParam = "partitions"
  val isSchemaFullParam = "is_schema_full"

  def apply(parameters: Map[String, String]): DefaultSourceParameters = {
    DefaultSourceParameters(
      path = parameters(pathParam),
      proxy = parameters(proxyParam),
      user = parameters.getOrElse(userParam, DefaultRpcCredentials.user),
      token = parameters.getOrElse(tokenParam, DefaultRpcCredentials.token),
      partitions = parameters.get(partitionsParam).map(_.toInt),
      isSchemaFull = parameters.get(isSchemaFullParam).forall(_.toBoolean),
    )
  }

  def apply(parameters: Map[String, String], user: String, token: String): DefaultSourceParameters = {
    DefaultSourceParameters(
      path = parameters(pathParam),
      proxy = parameters(proxyParam),
      user = parameters.getOrElse(userParam, user),
      token = parameters.getOrElse(tokenParam, token),
      partitions = parameters.get(partitionsParam).map(_.toInt),
      isSchemaFull = parameters.get(isSchemaFullParam).forall(_.toBoolean),
    )
  }

  def schemaHint(options: Map[String, String]): Option[StructType] = {
    val fields = options.collect{case (key, value) if key.contains("_hint") =>
        val name = key.dropRight("_hint".length)
        val `type` = SchemaConverter.sparkType(value)
        StructField(name, `type`.getOrElse(BinaryType))
    }

    if (fields.nonEmpty) {
      Some(StructType(fields.toSeq))
    } else {
      None
    }
  }

}
