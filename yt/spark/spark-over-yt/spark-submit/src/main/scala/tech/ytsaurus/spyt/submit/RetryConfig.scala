package tech.ytsaurus.spyt.submit

import scala.concurrent.duration._
import scala.language.postfixOps

case class RetryConfig(enableRetry: Boolean = true,
                       retryLimit: Int = 10,
                       retryInterval: Duration = 1 minute,
                       waitSubmissionIdRetryLimit: Int = 50) {
  def this(enableRetry: Boolean, retryLimit: Int, retryInterval: Duration) {
    this(enableRetry, retryLimit, retryInterval, 50)
  }
}

object RetryConfig {
  // for Python wrapper
  def durationFromSeconds(amount: Int): Duration = {
    amount.seconds
  }
}
