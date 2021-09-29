package ru.yandex.spark.yt.wrapper.dyntable

import org.slf4j.LoggerFactory
import ru.yandex.inside.yt.kosher.cypress.YPath
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeNodeUtils
import ru.yandex.inside.yt.kosher.impl.ytree.`object`.YTreeSerializer
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.{YTreeBinarySerializer, YTreeTextSerializer}
import ru.yandex.inside.yt.kosher.ytree.{YTreeMapNode, YTreeNode}
import ru.yandex.misc.reflection.ClassX
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.YtWrapper.{createTable, createTransaction}
import ru.yandex.spark.yt.wrapper.cypress.{YtAttributes, YtCypressUtils}
import ru.yandex.spark.yt.wrapper.table.YtTableSettings
import ru.yandex.yson.YsonConsumer
import ru.yandex.yt.ytclient.proxy.request.{GetTablePivotKeys, ReshardTable, WriteTable}
import ru.yandex.yt.ytclient.proxy.{ApiServiceTransaction, CompoundClient, ModifyRowsRequest, RetryPolicy, SelectRowsRequest}
import ru.yandex.yt.ytclient.rpc.AlwaysSwitchRpcFailoverPolicy
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.time.{Duration => JDuration}
import java.util.concurrent.{ExecutorService, Executors}
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
  val emptyPivotKey: PivotKey = serialiseYson(new YTreeBuilder().beginMap().endMap().build())
  private val executor = Executors.newSingleThreadExecutor

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
    yt
      .getTablePivotKeys(new GetTablePivotKeys(path.justPath().toString))
      .join()
      .asScala
  }

  def pivotKeys(path: String)(implicit yt: CompoundClient): Seq[PivotKey] = {
    pivotKeys(YPath.simple(formatPath(path)))
  }

  def pivotKeys(path: YPath)(implicit yt: CompoundClient): Seq[PivotKey] = {
    pivotKeysYson(path).map(serialiseYson)
  }

  def keyColumns(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Seq[String] = {
    keyColumns(attribute(path, YtAttributes.sortedBy, transaction))
  }

  def keyColumns(attr: YTreeNode): Seq[String] = {
    import scala.collection.JavaConverters._
    attr.asList().asScala.map(_.stringValue())
  }

  def keyColumns(attrs: Map[String, YTreeNode]): Seq[String] = {
    keyColumns(attrs(YtAttributes.sortedBy))
  }

  def mountTable(path: String)(implicit yt: CompoundClient): Unit = {
    log.debug(s"Mount table: $path")
    yt.mountTable(formatPath(path)).join()
  }

  def mountTableSync(path: String, timeout: Duration)(implicit yt: CompoundClient): Unit = {
    mountTable(path)
    waitState(path, TabletState.Mounted, timeout)
  }

  def unmountTableSync(path: String, timeout: Duration)(implicit yt: CompoundClient): Unit = {
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

  def isDynTablePrepared(path: String)(implicit yt: CompoundClient): Boolean = {
    exists(path) && tabletState(path) == TabletState.Mounted
  }

  def createDynTableAndMount(path: String,
                             schema: TableSchema,
                             settings: Map[String, Any] = Map.empty,
                             ignoreExisting: Boolean = true)
                            (implicit yt: CompoundClient): Unit = {
    val tableExists = exists(path)
    val tabletMounted = tableExists && tabletState(path) == TabletState.Mounted

    if (tableExists && tabletMounted && !ignoreExisting) {
      throw new RuntimeException("Table already exists")
    }

    if (!tableExists) createDynTable(path, schema, settings)
    if (!tabletMounted) mountAndWait(path)
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

  def mountAndWait(path: String)(implicit yt: CompoundClient): Unit = {
    mountTable(path)
    waitState(path, TabletState.Mounted, 10 seconds)
  }

  def selectRows(path: String, schema: TableSchema, condition: Option[String] = None,
                 parentTransaction: Option[ApiServiceTransaction] = None)(implicit yt: CompoundClient): Seq[YTreeMapNode] = {
    import scala.collection.JavaConverters._
    val request = SelectRowsRequest.of(s"""* from [${formatPath(path)}] ${condition.map("where " + _).mkString}""")
    waitState(path, TabletState.Mounted, 60 seconds)

    val rowsFuture = yt.retryWithTabletTransaction(
      _.selectRows(request),
      executor,
      RetryPolicy.attemptLimited(3, RetryPolicy.fromRpcFailoverPolicy(new AlwaysSwitchRpcFailoverPolicy))
    )

    val selected = rowsFuture.join()
    selected.getRows.asScala.map(x => x.toYTreeMap(schema)).toList
  }

  private def processModifyRowsRequest(request: ModifyRowsRequest,
                                       parentTransaction: Option[ApiServiceTransaction] = None)(implicit yt: CompoundClient) = {
    runUnderTransaction(parentTransaction)(transaction => {
      transaction.modifyRows(request).get(1, MINUTES)
    })
  }

  def runUnderTransaction[T](parent: Option[ApiServiceTransaction])
                            (f: ApiServiceTransaction => T)(implicit yt: CompoundClient): T = {
    val transaction = parent.getOrElse(createTransaction(parent = None, timeout = 1 minute, sticky = true))
    try {
      val res = f(transaction)
      if (parent.isEmpty) transaction.commit().join()
      res
    } catch {
      case e: Throwable =>
        try {
          transaction.abort().join()
        } catch {
          case e2: Throwable => log.error("Transaction abort failed with: " + e2.getMessage)
        }
        throw e
    }
  }

  def insertRows(path: String, schema: TableSchema, rows: java.util.List[java.util.List[Any]],
                 parentTransaction: Option[ApiServiceTransaction])(implicit yt: CompoundClient): Unit = {
    processModifyRowsRequest(new ModifyRowsRequest(formatPath(path), schema).addInserts(rows), parentTransaction)
  }

  def insertRows(path: String, schema: TableSchema, rows: Seq[Seq[Any]],
                 parentTransaction: Option[ApiServiceTransaction] = None)(implicit yt: CompoundClient): Unit = {
    import scala.collection.JavaConverters._

    processModifyRowsRequest(new ModifyRowsRequest(formatPath(path), schema).addInserts(rows.map(_.asJava).asJava), parentTransaction)
  }

  def updateRows(path: String, schema: TableSchema, map: java.util.Map[String, Any],
                 parentTransaction: Option[ApiServiceTransaction] = None)(implicit yt: CompoundClient): Unit = {
    processModifyRowsRequest(new ModifyRowsRequest(formatPath(path), schema).addUpdate(map), parentTransaction)
  }

  def deleteRow(path: String, schema: TableSchema, map: java.util.Map[String, Any],
                parentTransaction: Option[ApiServiceTransaction] = None)(implicit yt: CompoundClient): Unit = {
    processModifyRowsRequest(new ModifyRowsRequest(formatPath(path), schema).addDelete(map), parentTransaction)
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
    val request = new ReshardTable(YPath.simple(formatPath(path))).setSchema(schema)
    pivotKeys.foreach { key =>
      request.addPivotKey(key.asJava)
    }
    yt.reshardTable(request).join()
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
