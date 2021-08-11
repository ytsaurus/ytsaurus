package ru.yandex.spark.yt.submit

import scala.concurrent.duration._
import scala.language.postfixOps

case class RetryConfig(enableRetry: Boolean = true,
                       retryLimit: Int = 10,
                       retryInterval: Duration = 1 minute)

object RetryConfig {
  // for Python wrapper
  def durationFromSeconds(amount: Int): Duration = {
    amount.seconds
  }
}
