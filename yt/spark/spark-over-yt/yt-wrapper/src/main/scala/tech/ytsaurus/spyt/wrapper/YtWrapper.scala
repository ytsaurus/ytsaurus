package tech.ytsaurus.spyt.wrapper

import tech.ytsaurus.spyt.wrapper.client.YtClientUtils
import tech.ytsaurus.spyt.wrapper.cypress.YtCypressUtils
import tech.ytsaurus.spyt.wrapper.dyntable.{YtDynTableUtils, YtQueueUtils}
import tech.ytsaurus.spyt.wrapper.file.YtFileUtils
import tech.ytsaurus.spyt.wrapper.table.{YtTableAttributes, YtTableUtils}
import tech.ytsaurus.spyt.wrapper.transaction.YtTransactionUtils

object YtWrapper extends YtClientUtils
  with YtCypressUtils
  with YtTransactionUtils
  with YtFileUtils
  with YtTableUtils
  with YtTableAttributes
  with YtDynTableUtils
  with YtQueueUtils
  with LogLazy {

}
