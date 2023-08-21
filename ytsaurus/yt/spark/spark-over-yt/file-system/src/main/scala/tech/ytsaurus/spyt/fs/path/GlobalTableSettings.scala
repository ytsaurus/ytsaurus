package tech.ytsaurus.spyt.fs.path

import scala.collection.mutable

object GlobalTableSettings {
  private val _transactions = new ThreadLocal[mutable.HashMap[String, String]]

  private def transactions: mutable.HashMap[String, String] = Option(_transactions.get()).getOrElse{
    _transactions.set(mutable.HashMap.empty[String, String])
    _transactions.get()
  }

  def setTransaction(path: String, transaction: String): Unit = transactions += path -> transaction

  def getTransaction(path: String): Option[String] = transactions.get(path)

  def removeTransaction(path: String): Unit = transactions.remove(path)
}
