package ru.yandex.spark.yt.common.utils

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, Row}

class TopUdaf(schema: StructType, topColumns: Seq[String]) extends UserDefinedAggregateFunction {
  private val topIndices = topColumns.map(schema.fieldIndex)

  private val schemaLength = schema.length

  override def inputSchema: StructType = schema

  override def bufferSchema: StructType = schema

  override def dataType: DataType = schema

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    topIndices.foreach {index =>
      buffer(index) = null
    }
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    import Ordering.Implicits._
    val current = topIndices.map(buffer.getAs[String])
    val candidate = topIndices.map(input.getAs[String])
    if (current.forall(_ == null) || candidate < current) {
      copyRowToBuffer(buffer, input)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    import Ordering.Implicits._
    val top1 = topIndices.map(buffer1.getAs[String])
    val top2 = topIndices.map(buffer2.getAs[String])
    if (top1.forall(_ == null) || top2.exists(_ != null) && top2 < top1) {
      copyRowToBuffer(buffer1, buffer2)
    }
  }

  private def copyRowToBuffer(buffer: MutableAggregationBuffer, row: Row): Unit = {
    (0 until schemaLength).foreach { index =>
      buffer(index) = row.get(index)
    }
  }

  override def evaluate(buffer: Row): Any = {
    buffer
  }
}

object TopUdaf {
  def top(schema: StructType, topColumns: Seq[String], selectColumns: Seq[String]): Column = {
    val resSchema = StructType(selectColumns.map(schema(_)))
    val udaf = new TopUdaf(resSchema, topColumns)
    udaf(selectColumns.map(name => col(name)):_*)
  }

  def top(schema: StructType,
          topColumns: java.util.ArrayList[String],
          selectColumns: java.util.ArrayList[String]): Column = {
    import scala.collection.JavaConverters._
    top(schema, topColumns.asScala, selectColumns.asScala)
  }
}
