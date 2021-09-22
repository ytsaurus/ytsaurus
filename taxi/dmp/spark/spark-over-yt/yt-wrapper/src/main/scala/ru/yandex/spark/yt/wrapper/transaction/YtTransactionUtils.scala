package ru.yandex.spark.yt.wrapper.transaction

import java.time.{Duration => JDuration}

import org.slf4j.LoggerFactory
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.spark.yt.wrapper._
import ru.yandex.yt.ytclient.proxy.request._
import ru.yandex.yt.ytclient.proxy.{ApiServiceTransaction, CompoundClient}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{CancellationException, ExecutionContext, Future, Promise}
import scala.language.postfixOps
import scala.util.Random

trait YtTransactionUtils {
  self: LogLazy =>

  private val log = LoggerFactory.getLogger(getClass)

  def createTransaction(timeout: JDuration, sticky: Boolean)
                       (implicit yt: CompoundClient): ApiServiceTransaction = {
    createTransaction(None, toScalaDuration(timeout), sticky)
  }

  def createTransaction(parent: Option[String], timeout: Duration, sticky: Boolean = false)
                       (implicit yt: CompoundClient): ApiServiceTransaction = {
    log.debugLazy(s"Start transaction, parent $parent, timeout $timeout, sticky $sticky")
    val request = (if (sticky) StartTransaction.tablet() else StartTransaction.master())
      .setTransactionTimeout(toJavaDuration(timeout))
      .setTimeout(toJavaDuration(timeout))
      .setPing(true)
      .setPingPeriod(toJavaDuration(30 seconds))

    parent.foreach(p => request.setParentId(GUID.valueOf(p)))
    val tr = yt.startTransaction(request).join()
    tr
  }

  def abortTransaction(guid: String)(implicit yt: CompoundClient): Unit = {
    log.debugLazy(s"Abort transaction $guid")
    yt.abortTransaction(GUID.valueOf(guid)).join()
  }

  def commitTransaction(guid: String)(implicit yt: CompoundClient): Unit = {
    log.debugLazy(s"Commit transaction $guid")
    yt.commitTransaction(GUID.valueOf(guid)).join()
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
                     (implicit yt: CompoundClient, ec: ExecutionContext): Cancellable[Unit] = {
    @tailrec
    def ping(cancel: Future[Unit], retry: Int): Boolean = {
      try {
        if (!cancel.isCompleted) {
          tr.ping().join()
          true
        } else false
      } catch {
        case e: Throwable =>
          log.error(s"Error in ping transaction ${tr.getId}, ${e.getMessage},\n" +
            s"Suppressed: ${e.getSuppressed.map(_.getMessage).mkString("\n")}")
          if (retry > 0) {
            Thread.sleep(new Random().nextInt(2000) + 100)
            ping(cancel, retry - 1)
          } else false
      }
    }

    cancellable { cancel =>
      var success = true
      while (!cancel.isCompleted && success) {
        log.debugLazy(s"Ping transaction ${tr.getId}")
        success = ping(cancel, 3)
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

  implicit class RichReadFileRequest[T <: ReadFile](val request: T) {
    def optionalTransaction(transaction: Option[String]): T = {
      transaction.map { t =>
        request.setTransactionalOptions(new TransactionalOptions(GUID.valueOf(t))).asInstanceOf[T]
      }.getOrElse(request)
    }
  }

  implicit class RichStartOperationRequest[T <: StartOperation](val request: T) {
    def optionalTransaction(transaction: Option[String]): T = {
      transaction.map { t =>
        request.setTransactionOptions(new TransactionalOptions(GUID.valueOf(t))).asInstanceOf[T]
      }.getOrElse(request)
    }
  }

}
