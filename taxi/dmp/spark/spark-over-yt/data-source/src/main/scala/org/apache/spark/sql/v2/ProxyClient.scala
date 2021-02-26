package org.apache.spark.sql.v2

import scala.concurrent.Future

trait ProxyClient {

  def put(key: String, value: String): Unit

  def get(key: String): Option[String]

}

trait Proxy {
  def id: String

  def createClient: ProxyClient
}

trait DiscoveryClient {
  def listProxies: Seq[Proxy]

  def proxyLoad(proxyId: String): Double
}