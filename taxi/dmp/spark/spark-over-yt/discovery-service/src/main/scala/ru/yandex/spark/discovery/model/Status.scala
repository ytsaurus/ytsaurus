package ru.yandex.spark.discovery.model

sealed trait Status

object Status {
  case object Success extends Status

  case class Failure(message: String) extends Status
}
