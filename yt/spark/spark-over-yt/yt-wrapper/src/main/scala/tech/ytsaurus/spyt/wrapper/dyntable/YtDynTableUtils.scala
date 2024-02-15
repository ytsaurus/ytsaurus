package tech.ytsaurus.spyt.wrapper.dyntable

import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.wrapper.YtJavaConverters._
import tech.ytsaurus.spyt.wrapper.YtWrapper.createTable
import tech.ytsaurus.spyt.wrapper.cypress.{YtAttributes, YtCypressUtils}
import tech.ytsaurus.spyt.wrapper.table.YtTableSettings
import tech.ytsaurus.client.request.{GetTablePivotKeys, ModifyRowsRequest, ReshardTable, SelectRowsRequest}
import tech.ytsaurus.client.rows.UnversionedRowset
import tech.ytsaurus.client.{ApiServiceTransaction, CompoundClient, RetryPolicy}
import tech.ytsaurus.client.rpc.AlwaysSwitchRpcFailoverPolicy
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.ysontree.{YTreeBinarySerializer, YTreeBuilder, YTreeMapNode, YTreeNode}

import java.io.ByteArrayOutputStream
import java.time.{Duration => JDuration}
import java.util.concurrent.{CompletableFuture, Executors, ThreadFactory}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

trait YtDynTableUtils {
  self: YtCypressUtils =>

  private val log = LoggerFactory.getLogger(getClass)

  type PivotKey = Array[Byte]
  val emptyPivotKey: PivotKey = serialiseYson(new YTreeBuilder().beginList().endList().build())
  private val executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
    override def newThread(runnable: Runnable): Thread = {
      val thread: Thread = Executors.defaultThreadFactory().newThread(runnable)
      thread.setDaemon(true)
      thread
    }
  })

  def serialiseYson(node: YTreeNode): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    try {
      YTreeBinarySerializer.serialize(node, baos)
      baos.toByteArray
    } finally {
      baos.close()
    }
  }

  def pivotKeysYson(path: YPath)(implicit yt: CompoundClient): Seq[YTreeNode] = {
    import scala.collection.JavaConverters._
    log.debug(s"Get pivot keys for $path")
    val res = yt.getTablePivotKeys(
        GetTablePivotKeys.builder().setPath(path.justPath().toString).setRepresentKeyAsList(true).build()
      )
      .join()
      .asScala
    res
  }

  def pivotKeys(path: String)(implicit yt: CompoundClient): Seq[PivotKey] = {
    pivotKeys(YPath.simple(formatPath(path)))
  }

  def pivotKeys(path: YPath)(implicit yt: CompoundClient): Seq[PivotKey] = {
    pivotKeysYson(path).map(serialiseYson)
  }

  def keyColumns(path: YPath, transaction: Option[String] = None)(implicit yt: CompoundClient): Seq[String] = {
    keyColumns(attribute(path, YtAttributes.keyColumns, transaction))
  }

  def keyColumns(attr: YTreeNode): Seq[String] = {
    import scala.collection.JavaConverters._
    attr.asList().asScala.map(_.stringValue())
  }

  def mountTable(path: String)(implicit yt: CompoundClient): Unit = {
    log.debug(s"Mount table: $path")
    yt.mountTable(formatPath(path)).join()
  }

  def mountTableSync(path: String, timeout: Duration = 20 seconds)(implicit yt: CompoundClient): Unit = {
    mountTable(path)
    waitState(path, TabletState.Mounted, timeout)
  }

  def unmountTableSync(path: String, timeout: Duration = 20 seconds)(implicit yt: CompoundClient): Unit = {
    unmountTable(path)
    waitState(path, TabletState.Unmounted, timeout)
  }

  def unmountTable(path: String)(implicit yt: CompoundClient): Unit = {
    log.debug(s"Unmount table: $path")
    yt.unmountTable(formatPath(path)).join()
  }

  def waitState(path: String, state: TabletState, timeout: JDuration)
               (implicit yt: CompoundClient): Unit = {
    waitState(path, state, toScalaDuration(timeout)).get
  }

  def waitState(path: String, state: TabletState, timeout: Duration)
               (implicit yt: CompoundClient): Try[Unit] = {
    @tailrec
    def waitUnmount(timeoutMillis: Long): Try[Unit] = {
      tabletState(path) match {
        case s if s == state => Success()
        case _ if timeoutMillis > 0 =>
          Thread.sleep(1000)
          waitUnmount(timeoutMillis - 1000)
        case _ => Failure(new TimeoutException)
      }
    }

    waitUnmount(timeout.toMillis)
  }

  def isDynamicTable(path: String)(implicit yt: CompoundClient): Boolean = {
    exists(path) && attribute(path, "dynamic").boolValue()
  }

  def isDynTablePrepared(path: String)(implicit yt: CompoundClient): Boolean = {
    exists(path) && isMounted(path)
  }

  def isMounted(path: String)(implicit yt: CompoundClient): Boolean = {
    tabletState(path) == TabletState.Mounted
  }

  def createDynTableAndMount(path: String,
                             schema: TableSchema,
                             settings: Map[String, Any] = Map.empty,
                             ignoreExisting: Boolean = true)
                            (implicit yt: CompoundClient): Unit = {
    val tableExists = exists(path)
    val tabletMounted = tableExists && isMounted(path)

    if (tableExists && tabletMounted && !ignoreExisting) {
      throw new RuntimeException("Table already exists")
    }

    if (!tableExists) createDynTable(path, schema, settings)
    if (!tabletMounted) mountTableSync(path)
  }

  private val cachedCreatedTables = mutable.Queue.empty[String]
  private val cachedCreatedTablesMaxSize = 10

  def createDynTableAndMountCached(path: String,
                                   schema: TableSchema,
                                   settings: Map[String, Any] = Map.empty,
                                   ignoreExisting: Boolean = true)
                                  (implicit yt: CompoundClient): Unit = {
    if (!cachedCreatedTables.contains(path)) {
      createDynTableAndMount(path, schema, settings, ignoreExisting)
      cachedCreatedTables.enqueue(path)
      if (cachedCreatedTables.size > cachedCreatedTablesMaxSize) {
        cachedCreatedTables.dequeue()
      }
    }
  }

  def createDynTable(path: String, schema: TableSchema, settings: Map[String, Any] = Map.empty)(implicit yt: CompoundClient): Unit = {
    createTable(path, new YtTableSettings {
      override def ytSchema: YTreeNode = schema.toYTree

      override def optionsAny: Map[String, Any] = settings + ("dynamic" -> "true")
    })
  }

  private def selectRowsRequest(query: String, path: String,
                                transaction: Option[ApiServiceTransaction] = None)
                               (implicit yt: CompoundClient): Seq[YTreeMapNode] = {
    import scala.collection.JavaConverters._
    val request = SelectRowsRequest.of(query)

    waitState(path, TabletState.Mounted, 60 seconds)
    val f: ApiServiceTransaction => UnversionedRowset = _.selectRows(request).get(10, MINUTES)
    val selected = if (transaction.isEmpty) {
      runWithRetry(f)
    } else {
      f(transaction.get)
    }
    selected.getYTreeRows.asScala.toList
  }

  def selectRows(path: String, condition: Option[String] = None,
                 transaction: Option[ApiServiceTransaction] = None,
                 columns: Seq[String] = Nil)(implicit yt: CompoundClient): Seq[YTreeMapNode] = {
    selectRowsRequest(
      s"""${ if (columns.nonEmpty) columns.mkString(", ") else "*" } from [${formatPath(path)}] ${condition.map("where " + _).mkString}""",
      path, transaction)
  }

  def countRows(path: String, condition: Option[String] = None,
                 transaction: Option[ApiServiceTransaction] = None)(implicit yt: CompoundClient): Long = {
    selectRowsRequest(
      s"""SUM(1) as count from [${formatPath(path)}] ${condition.map("where " + _).mkString} group by 1""",
      path, transaction).headOption.map(_.getLong("count")).getOrElse(0L)
  }

  private def processModifyRowsRequest(request: ModifyRowsRequest,
                                       transaction: Option[ApiServiceTransaction] = None)
                                      (implicit yt: CompoundClient): Unit = {
    val f: ApiServiceTransaction => Unit = _.modifyRows(request).get(1, MINUTES)
    if (transaction.isEmpty) {
      runWithRetry(f)
    } else {
      f(transaction.get)
    }
  }

  def runWithRetry[T](f: ApiServiceTransaction => T)(implicit yt: CompoundClient): T = {
    val rowsFuture = yt.retryWithTabletTransaction(
      transaction => CompletableFuture.supplyAsync(() => f(transaction)),
      executor,
      RetryPolicy.attemptLimited(3, RetryPolicy.fromRpcFailoverPolicy(new AlwaysSwitchRpcFailoverPolicy))
    ).join()
    rowsFuture
  }

  def insertRows(path: String, schema: TableSchema, rows: java.util.List[java.util.List[Any]],
                 parentTransaction: Option[ApiServiceTransaction])(implicit yt: CompoundClient): Unit = {
    processModifyRowsRequest(
      ModifyRowsRequest.builder()
        .setPath(formatPath(path))
        .setSchema(schema)
        .addInserts(rows)
        .build(),
      parentTransaction)
  }

  def insertRows(path: String, schema: TableSchema, rows: Seq[Seq[Any]],
                 parentTransaction: Option[ApiServiceTransaction] = None)(implicit yt: CompoundClient): Unit = {
    import scala.collection.JavaConverters._
    processModifyRowsRequest(
      ModifyRowsRequest.builder()
        .setPath(formatPath(path))
        .setSchema(schema)
        .addInserts(rows.map(_.asJava).asJava)
        .build(),
      parentTransaction)
  }

  def updateRow(path: String, schema: TableSchema, map: java.util.Map[String, Any],
                parentTransaction: Option[ApiServiceTransaction] = None)(implicit yt: CompoundClient): Unit = {
    processModifyRowsRequest(
      ModifyRowsRequest.builder()
        .setPath(formatPath(path))
        .setSchema(schema)
        .addUpdate(map)
        .build(),
      parentTransaction
    )
  }

  def deleteRow(path: String, schema: TableSchema, map: java.util.Map[String, Any],
                parentTransaction: Option[ApiServiceTransaction] = None)(implicit yt: CompoundClient): Unit = {
    processModifyRowsRequest(
      ModifyRowsRequest.builder()
        .setPath(formatPath(path))
        .setSchema(schema)
        .addDelete(map)
        .build(),
      parentTransaction
    )
  }

  def deleteRows(path: String, schema: TableSchema, rows: Seq[java.util.Map[String, Any]],
                parentTransaction: Option[ApiServiceTransaction] = None)(implicit yt: CompoundClient): Unit = {
    val request = rows.foldLeft(
      ModifyRowsRequest.builder()
        .setPath(formatPath(path))
        .setSchema(schema)
    ) {
      case (req, next) => req.addDelete(next)
    }
    processModifyRowsRequest(request.build(), parentTransaction)
  }

  def tabletState(path: String)(implicit yt: CompoundClient): TabletState = {
    TabletState.fromString(attribute(formatPath(path), YtAttributes.tabletState).stringValue())
  }

  def remountTable(path: String)(implicit yt: CompoundClient): Unit = {
    yt.remountTable(formatPath(path)).join()
  }

  def maxAvailableTimestamp(path: YPath, transaction: Option[String] = None)
                           (implicit yt: CompoundClient): Long = {
    if (isDynamicStoreReadEnabled(path, transaction)) {
      yt.generateTimestamps().join().getValue
    } else {
      attribute(path, "unflushed_timestamp", transaction).longValue() - 1
    }
  }

  def isDynamicStoreReadEnabled(path: YPath, transaction: Option[String] = None)
                               (implicit yt: CompoundClient): Boolean = {
    attribute(path, "enable_dynamic_store_read", transaction).boolValue()
  }

  def reshardTable(path: String, schema: TableSchema, pivotKeys: Seq[Seq[Any]])
                  (implicit yt: CompoundClient): Unit = {
    import scala.collection.JavaConverters._
    val rawRequest = ReshardTable.builder()
      .setPath(YPath.simple(formatPath(path)))
      .setSchema(schema)
    pivotKeys.foreach { key =>
      rawRequest.addPivotKey(key.asJava)
    }
    yt.reshardTable(rawRequest.build()).join()
  }

  sealed abstract class TabletState(val name: String)

  object TabletState {

    case object Mounted extends TabletState("mounted")

    case object Unmounted extends TabletState("unmounted")

    case object Unknown extends TabletState("")

    def fromString(str: String): TabletState = {
      Seq(Mounted, Unmounted).find(_.name == str).getOrElse(Unknown)
    }
  }

}
