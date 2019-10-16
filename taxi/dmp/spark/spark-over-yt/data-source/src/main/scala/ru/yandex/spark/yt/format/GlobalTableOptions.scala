package ru.yandex.spark.yt.format

import org.apache.spark.sql.types.StructType

import scala.collection.mutable

object GlobalTableOptions {
  private val jobSchemas = mutable.HashMap.empty[String, StructType]
  private val filesCount = mutable.HashMap.empty[String, Int]
  private val _transactions = new ThreadLocal[mutable.HashMap[String, String]]

  private def transactions: mutable.HashMap[String, String] = Option(_transactions.get()).getOrElse{
    _transactions.set(mutable.HashMap.empty[String, String])
    _transactions.get()
  }

  def getSchema(jobId: String): StructType = jobSchemas(jobId)

  def setSchema(jobId: String, schema: StructType): Unit = jobSchemas += (jobId -> schema)

  def removeSchema(jobId: String): Unit = jobSchemas.remove(jobId)


  def setTransaction(path: String, transaction: String): Unit = transactions += path -> transaction

  def getTransaction(path: String): Option[String] = transactions.get(path)

  def removeTransaction(path: String): Unit = transactions.remove(path)


  def getFilesCount(path: String): Option[Int] = filesCount.get(path)

  def setFilesCount(path: String, count: Int): Unit = filesCount += (path -> count)

  def removeFilesCount(path: String): Unit = filesCount.remove(path)
}
