package ru.yandex.spark.yt.fs

import scala.collection.mutable

object GlobalTableSettings {
  private val filesCount = mutable.HashMap.empty[String, Int]
  private val _transactions = new ThreadLocal[mutable.HashMap[String, String]]

  private def transactions: mutable.HashMap[String, String] = Option(_transactions.get()).getOrElse{
    _transactions.set(mutable.HashMap.empty[String, String])
    _transactions.get()
  }

  def setTransaction(path: String, transaction: String): Unit = transactions += path -> transaction

  def getTransaction(path: String): Option[String] = transactions.get(path)

  def removeTransaction(path: String): Unit = transactions.remove(path)


  def getFilesCount(path: String): Option[Int] = filesCount.get(path)

  def setFilesCount(path: String, count: Int): Unit = filesCount += (path -> count)

  def removeFilesCount(path: String): Unit = filesCount.remove(path)
}
