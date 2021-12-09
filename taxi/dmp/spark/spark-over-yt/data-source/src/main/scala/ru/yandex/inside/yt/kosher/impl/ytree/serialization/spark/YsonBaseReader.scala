package ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YsonTags
import ru.yandex.yson.YsonError

import scala.annotation.tailrec

trait YsonBaseReader {
  def parseToken(allowEof: Boolean): Byte

  def parseNode(first: Byte, allowEof: Boolean, dataType: IndexedDataType): Any

  def unexpectedToken(token: Byte, expected: String): Unit = {
    throw new YsonError(String.format(
      "Unexpected token '%s', expected '%s'", token.toChar.toString, expected
    ))
  }

  def readList(endToken: Byte, allowEof: Boolean)
              (elementConsumer: (Int, Byte) => Unit): Unit = {
    @tailrec
    def read(index: Int, requireSeparator: Boolean = false): Unit = {
      val token = parseToken(allowEof)
      token match {
        case t if t == endToken => // end reading
        case YsonTags.ITEM_SEPARATOR => read(index)
        case _ =>
          if (requireSeparator) unexpectedToken(token, "ITEM_SEPARATOR")
          elementConsumer(index, token)
          read(index + 1, requireSeparator = true)
      }
    }

    read(0)
  }

  def readMap(endToken: Byte, allowEof: Boolean)
             (keyParser: Byte => Any)(valueParser: (Byte, Any) => Any)
             (keyValueConsumer: (Any, Any) => Unit): Unit = {
    @tailrec
    def read(key: Option[Any] = None,
             requireKeyValueSeparator: Boolean = false,
             requireItemSeparator: Boolean = false): Unit = {
      val token = parseToken(allowEof)
      token match {
        case t if t == endToken => // do nothing
        case YsonTags.KEY_VALUE_SEPARATOR =>
          if (requireItemSeparator) unexpectedToken(YsonTags.KEY_VALUE_SEPARATOR, "ITEM_SEPARATOR")
          read(key, requireKeyValueSeparator = false, requireItemSeparator)
        case YsonTags.ITEM_SEPARATOR =>
          if (requireKeyValueSeparator) unexpectedToken(YsonTags.ITEM_SEPARATOR, "KEY_VALUE_SEPARATOR")
          read(key, requireKeyValueSeparator, requireItemSeparator = false)
        case token =>
          if (requireItemSeparator) unexpectedToken(token, "ITEM_SEPARATOR")
          if (requireKeyValueSeparator) unexpectedToken(token, "KEY_VALUE_SEPARATOR")
          key match {
            case Some(k) =>
              val value = valueParser(token, k)
              keyValueConsumer(k, value)
              read(None, requireItemSeparator = true)
            case None =>
              val k = keyParser(token)
              read(Some(k), requireKeyValueSeparator = true)
          }
      }
    }

    read(None)
  }


}
