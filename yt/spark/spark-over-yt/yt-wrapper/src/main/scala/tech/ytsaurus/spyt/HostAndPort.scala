package tech.ytsaurus.spyt

import java.net.InetSocketAddress

case class HostAndPort(host: String, port: Int) {
  def toAddress: InetSocketAddress = new InetSocketAddress(host, port)
  def asString: String = s"$hostText:$port"

  def hostText: String = {
    if (host.contains(":") && !host.startsWith("[")) {
      s"[$host]"
    } else {
      s"$host"
    }
  }

  override def toString: String = asString
}

// Replacement for Guava HostAndPort with simplified checks
object HostAndPort {
  private val bracketedHostAndPort = "^\\[(.*?)](:(\\d+))?$".r
  private val unbracketedHostAndPort = "^(.*?)(:(\\d+))?$".r

  private def parsePort(port: String): Int =
    try {
      val p = port.toInt
      if (p >= 0 && p <= 65535) p
      else throw new IllegalArgumentException(s"Invalid port number $p")
    }
    catch {
      case ex: NumberFormatException => throw new IllegalArgumentException(s"Unparseable port number $port", ex)
    }

  def fromString(hostPortString: String): HostAndPort =
    hostPortString match {
      case bracketedHostAndPort(host, _, port) => HostAndPort(host, parsePort(port))
      case unbracketedHostAndPort(host, _, port) => HostAndPort(host, parsePort(port))
      case _ => throw new IllegalArgumentException(s"Unable to parse $hostPortString")
    }
}


