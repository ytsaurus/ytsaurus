package org.apache.livy.repl

import io.netty.channel.ChannelHandlerContext
import org.apache.livy.rsc.driver.{QueryPlan, SparkEntries, StatementSpyt}
import org.apache.livy.rsc.{BaseProtocol, RSCConf, ReplJobResults}
import org.apache.spark.{SparkConf, StoreUtils}
import tech.ytsaurus.spyt.patch.annotations.{OriginClass, Subclass}

@Subclass
@OriginClass("org.apache.livy.repl.ReplDriver")
class ReplDriverSpyt(conf: SparkConf, livyConf: RSCConf) extends ReplDriver(conf, livyConf) {
  override def handle(ctx: ChannelHandlerContext, msg: BaseProtocol.GetReplJobResults): ReplJobResults = {
    val jobResults = super.handle(ctx, msg)
    jobResults.statements.foreach { s =>
      s.asInstanceOf[StatementSpyt].setPlan(queryPlan(s.id))
    }
    jobResults
  }

  private def getEntries: SparkEntries = {
    val entries = classOf[org.apache.livy.repl.Session].getDeclaredField("entries")
    entries.setAccessible(true)
    val result = entries.get(session).asInstanceOf[SparkEntries]
    entries.setAccessible(false)
    result
  }

  private def queryPlan(stmtId: Int): QueryPlan = {
    val entries = getEntries
    val jobIds = entries.sc().sc.statusTracker.getJobIdsForGroup(stmtId.toString)
    val store = StoreUtils.getStatusStore(entries.sc().sc)
    val executionIds = jobIds.flatMap { id => store.asOption(store.jobWithAssociatedSql(id)).flatMap(_._2) }.distinct
    if (executionIds.isEmpty) {
      logger.info(s"No SQL executions found for statement $stmtId")
      null
    } else {
      if (executionIds.length > 1) {
        logger.warn(s"Found more than 1 satisfying execution ids: $executionIds for statement $stmtId")
      }
      val executionId = executionIds.head
      val sqlStore = entries.sparkSession().sharedState.statusStore
      val metrics = sqlStore.executionMetrics(executionId)
      val graph = sqlStore.planGraph(executionId)
      val dotContent = graph.makeDotFile(metrics)
      val metadata = graph.allNodes.sortBy(_.id).map(_.desc)
      new QueryPlan(dotContent, metadata.toArray)
    }
  }
}
