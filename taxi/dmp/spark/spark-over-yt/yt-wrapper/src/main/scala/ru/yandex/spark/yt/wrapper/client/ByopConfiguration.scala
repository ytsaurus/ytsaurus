package ru.yandex.spark.yt.wrapper.client

import com.google.common.net.HostAndPort

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

case class ByopConfiguration(enabled: Boolean,
                             remote: ByopRemoteConfiguration)

object ByopConfiguration {
  val DISABLED = ByopConfiguration(enabled = false,
    ByopRemoteConfiguration(enabled = false, emptyWorkersListStrategy = EmptyWorkersListStrategy.Default))
}

case class ByopRemoteConfiguration(enabled: Boolean,
                                   emptyWorkersListStrategy: EmptyWorkersListStrategy)

sealed abstract class EmptyWorkersListStrategy(val name: String) extends Serializable {
  def use(client: MasterWrapperClient): Option[HostAndPort]
}

object EmptyWorkersListStrategy {
  private def waitProxies(client: MasterWrapperClient, timeout: Duration): Option[HostAndPort] = {
    @tailrec
    def inner(timeout: Long): Option[HostAndPort] = {
      val start = System.currentTimeMillis()
      client.discoverProxies match {
        case Success(Nil) | Failure(_) =>
          if (timeout > 0) {
            inner(timeout - (System.currentTimeMillis() - start))
          } else None
        case Success(_) => Some(client.endpoint)
      }
    }

    inner(timeout.toMillis)
  }

  case object Fallback extends EmptyWorkersListStrategy("fallback") {
    override def use(client: MasterWrapperClient): Option[HostAndPort] = None
  }

  case class WaitAndFallback(timeout: Duration) extends EmptyWorkersListStrategy("waitAndFallback") {
    override def use(client: MasterWrapperClient): Option[HostAndPort] = {
      waitProxies(client, timeout).orElse(Fallback.use(client))
    }
  }

  case object Fail extends EmptyWorkersListStrategy("fail") {
    override def use(client: MasterWrapperClient): Option[HostAndPort] = {
      throw new IllegalStateException("BYOP proxies is not available")
    }
  }

  case class WaitAndFail(timeout: Duration) extends EmptyWorkersListStrategy("waitAndFail") {
    override def use(client: MasterWrapperClient): Option[HostAndPort] = {
      waitProxies(client, timeout).orElse(Fail.use(client))
    }
  }

  val Default = EmptyWorkersListStrategy.WaitAndFallback(3 minutes)

  def fromString(name: String, timeout: Duration): EmptyWorkersListStrategy = {
    Seq(Fallback, WaitAndFallback(timeout), Fail, WaitAndFail(timeout))
      .find(_.name == name)
      .getOrElse(throw new IllegalArgumentException(s"Unknown strategy name: $name"))
  }
}
