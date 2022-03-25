package ru.yandex.spark.yt.fs.conf

class MultiConfigEntry[T](val prefix: String,
                          val postfix: String,
                          val default: Option[T],
                          val entryFactory: (String, Option[T]) => ConfigEntry[T]) {
  def get(configKeys: Seq[String], config: ConfProvider): Map[String, T] = {
    val defaultEntry = entryFactory(s"$prefix.$postfix", default)
    val defaultValue = config.ytConf(defaultEntry)
    configKeys.flatMap {
      case key if key.startsWith(prefix) =>
        val removedPrefix = key.drop(prefix.length + 1)
        if (removedPrefix != postfix && removedPrefix.endsWith(postfix)) {
          val name = removedPrefix.dropRight(postfix.length + 1)
          val entry = entryFactory(key, Some(defaultValue))
          Some(name -> config.ytConf(entry))
        } else None
      case _ => None
    }.toMap.withDefaultValue(defaultValue)
  }

}
