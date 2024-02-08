package tech.ytsaurus.spyt.test

import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.dyntable.ConsumerUtils

import scala.annotation.tailrec

trait QueueTestUtils {
  self: LocalYtClient with DynTableTestUtils =>

  def prepareConsumer(consumerPath: String, queuePath: String): Unit = {
    if (!YtWrapper.exists(consumerPath)) {
      YtWrapper.createDynTable(consumerPath, ConsumerUtils.CONSUMER_SCHEMA, Map("treat_as_queue_consumer" -> true))
    }
    YtWrapper.registerQueueConsumer(YPath.simple(consumerPath), YPath.simple(queuePath))
    if (!YtWrapper.isMounted(consumerPath)) {
      YtWrapper.mountTableSync(consumerPath)
    }
  }

  @tailrec
  final def waitQueueRegistration(queuePath: String, retries: Int = 10): Unit = {
    if (retries == 0) {
      throw new IllegalStateException("Queue is not registered after waiting")
    }
    try {
      YtWrapper.attribute(queuePath, "queue_partitions")
    } catch {
      case e: Throwable =>
        println("Waiting queue registration")
        Thread.sleep(2000)
        waitQueueRegistration(queuePath, retries - 1)
    }
  }
}
