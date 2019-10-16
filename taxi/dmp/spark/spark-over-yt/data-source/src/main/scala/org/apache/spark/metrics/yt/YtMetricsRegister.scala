package org.apache.spark.metrics.yt

import com.codahale.metrics.{Counter, Timer}
import org.apache.log4j.Logger
import org.apache.spark.SparkEnv

object YtMetricsRegister {
  lazy val ytMetricsSource = new YtMetricsSource
  private val log = Logger.getLogger(getClass)

  private var _initialized = false

  def register(): Unit = synchronized {
    if (!_initialized) {
      SparkEnv.get.metricsSystem.registerSource(ytMetricsSource)
      _initialized = true
    }
  }

  def time[T](timer: Timer, sumTimer: Counter)(f: => T): T = {
    if (log.isDebugEnabled) {
      val start = System.currentTimeMillis()
      val result = timer.time{() => f}
      val end = System.currentTimeMillis()
      sumTimer.inc(end - start)
      result
    } else {
      f
    }
  }

}
