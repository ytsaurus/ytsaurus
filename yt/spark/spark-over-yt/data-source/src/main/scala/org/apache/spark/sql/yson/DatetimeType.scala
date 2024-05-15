package org.apache.spark.sql.yson

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.{DataType, LongType, SQLUserDefinedType, UserDefinedType}
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import tech.ytsaurus.spyt.common.utils.DateTimeTypesConverter.{localDateTimeToLong, longToDatetime}

import java.time.{LocalDateTime, ZoneId, ZoneOffset}


class DatetimeType extends UserDefinedType[Datetime] {
  override def pyUDT: String = "spyt.types.DatetimeType"

  override def sqlType: DataType = LongType

  override def serialize(d: Datetime): Any = {
    localDateTimeToLong(d.datetime)
  }

  override def deserialize(datum: Any): Datetime = {
    datum match {
      case t: java.lang.Long => new Datetime(LocalDateTime.ofEpochSecond(t, 0, ZoneOffset.UTC))
      case _ => throw new AnalysisException(
        "Deserialization error: Expected java.lang.Long but got datum of type "
          + datum.getClass
      )
    }
  }

  override def userClass: Class[Datetime] = classOf[Datetime]

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("pyClass" -> pyUDT) ~
      ("serializedClass" -> serializedPyClass) ~
      ("sqlType" -> sqlType.jsonValue)
  }

  override def catalogString: String = "datetime"

}

@SQLUserDefinedType(udt = classOf[DatetimeType])
case class Datetime(datetime: LocalDateTime) extends Serializable {
  def toLong: Long = {
    val instant = datetime.toInstant(ZoneOffset.UTC)
    instant.getEpochSecond
  }

  override def toString: String = datetime.toString
}

object Datetime {
  def apply(value: Long): Datetime = {
    Datetime(LocalDateTime.ofEpochSecond(value, 0, ZoneOffset.UTC))
  }
}
