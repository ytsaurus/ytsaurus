package ru.yandex.spark.yt.conf

object SparkYtConfiguration {
  object Write {
    private val prefix = "write"

    case object BatchSize extends IntConfigEntry(s"$prefix.batchSize", Some(500000))
    case object MiniBatchSize extends IntConfigEntry(s"$prefix.miniBatchSize", Some(1000))
    case object Timeout extends IntConfigEntry(s"$prefix.timeout", Some(60))
  }

  object Read {
    private val prefix = "read"

    case object VectorizedCapacity extends IntConfigEntry(s"$prefix.vectorized.capacity", Some(1000))
  }

}
