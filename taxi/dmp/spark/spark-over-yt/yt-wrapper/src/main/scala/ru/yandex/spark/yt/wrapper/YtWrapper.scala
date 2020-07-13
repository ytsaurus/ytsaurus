package ru.yandex.spark.yt.wrapper

import ru.yandex.spark.yt.wrapper.client.YtClientUtils
import ru.yandex.spark.yt.wrapper.cypress.YtCypressUtils
import ru.yandex.spark.yt.wrapper.dyntable.YtDynTableUtils
import ru.yandex.spark.yt.wrapper.file.YtFileUtils
import ru.yandex.spark.yt.wrapper.table.{YtTableAttributes, YtTableUtils}
import ru.yandex.spark.yt.wrapper.transaction.YtTransactionUtils

object YtWrapper extends YtClientUtils
  with YtCypressUtils
  with YtTransactionUtils
  with YtFileUtils
  with YtTableUtils
  with YtTableAttributes
  with YtDynTableUtils
  with LogLazy {

}
