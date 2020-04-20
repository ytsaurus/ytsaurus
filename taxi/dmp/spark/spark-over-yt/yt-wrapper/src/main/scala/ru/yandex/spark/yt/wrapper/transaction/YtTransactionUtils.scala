package ru.yandex.spark.yt.wrapper.transaction

import org.apache.log4j.Logger
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.spark.yt.wrapper._
import ru.yandex.yt.rpcproxy.ETransactionType.TT_MASTER
import ru.yandex.yt.ytclient.proxy.request.{GetLikeReq, MutateNode, TransactionalOptions, WriteFile}
import ru.yandex.yt.ytclient.proxy.{ApiServiceTransaction, ApiServiceTransactionOptions, YtClient}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{CancellationException, ExecutionContext, Future, Promise}
import scala.language.postfixOps
import scala.util.Random

trait YtTransactionUtils {
  self: LogLazy =>

  private val log = Logger.getLogger(getClass)

  def createTransaction(parent: Option[String], timeout: Duration)(implicit yt: YtClient): ApiServiceTransaction = {
    val options = new ApiServiceTransactionOptions(TT_MASTER)
      .setTimeout(toJavaDuration(timeout))
      .setSticky(false)
      .setPing(true)
      .setPingPeriod(toJavaDuration(30 seconds))
    parent.foreach(p => options.setParentId(GUID.valueOf(p)))
    val tr = yt.startTransaction(options).join()
    tr
  }

  def abortTransaction(guid: String)(implicit yt: YtClient): Unit = {
    yt.abortTransaction(GUID.valueOf(guid), true).join()
  }

  def commitTransaction(guid: String)(implicit yt: YtClient): Unit = {
    yt.commitTransaction(GUID.valueOf(guid), true).join()
  }

  type Cancellable[T] = (Promise[Unit], Future[T])

  def cancellable[T](f: Future[Unit] => T)(implicit ec: ExecutionContext): Cancellable[T] = {
    val cancel = Promise[Unit]()
    val fut = Future {
      val res = f(cancel.future)
      if (!cancel.tryFailure(new Exception)) {
        throw new CancellationException
      }
      res
    }
    (cancel, fut)
  }

  def pingTransaction(tr: ApiServiceTransaction, interval: Duration)
                     (implicit yt: YtClient, ec: ExecutionContext): Cancellable[Unit] = {
    @tailrec
    def ping(cancel: Future[Unit], retry: Int): Unit = {
      try {
        if (!cancel.isCompleted) {
          tr.ping().join()
        }
      } catch {
        case e: Throwable =>
          log.error(s"Error in ping transaction ${tr.getId}, ${e.getMessage},\n" +
            s"Suppressed: ${e.getSuppressed.map(_.getMessage).mkString("\n")}")
          if (retry > 0) {
            Thread.sleep(new Random().nextInt(2000) + 100)
            ping(cancel, retry - 1)
          }
      }
    }

    cancellable { cancel =>
      while (!cancel.isCompleted) {
        log.debugLazy(s"Ping transaction ${tr.getId}")
        ping(cancel, 3)
        Thread.sleep(interval.toMillis)
      }
      log.debugLazy(s"Ping transaction ${tr.getId} cancelled")
    }
  }

  implicit class RichGetLikeRequest[T <: GetLikeReq[_]](val request: T) {
    def optionalTransaction(transaction: Option[String]): T = {
      transaction.map { t =>
        request.setTransactionalOptions(new TransactionalOptions(GUID.valueOf(t))).asInstanceOf[T]
      }.getOrElse(request)
    }
  }

  implicit class RichMutateNodeRequest[T <: MutateNode[_]](val request: T) {
    def optionalTransaction(transaction: Option[String]): T = {
      transaction.map { t =>
        request.setTransactionalOptions(new TransactionalOptions(GUID.valueOf(t))).asInstanceOf[T]
      }.getOrElse(request)
    }
  }

  implicit class RichWriteFileRequest[T <: WriteFile](val request: T) {
    def optionalTransaction(transaction: Option[String]): T = {
      transaction.map { t =>
        request.setTransactionalOptions(new TransactionalOptions(GUID.valueOf(t))).asInstanceOf[T]
      }.getOrElse(request)
    }
  }

}
