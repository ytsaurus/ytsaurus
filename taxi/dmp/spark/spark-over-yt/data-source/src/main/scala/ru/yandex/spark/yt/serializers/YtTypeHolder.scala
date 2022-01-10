package ru.yandex.spark.yt.serializers

import ru.yandex.type_info.{TiType, VariantType}
import ru.yandex.spark.yt.serializers.YtTypeHolder.{empty, getElementByIndex, getMemberByIndex, getMemberByName}

import scala.annotation.tailrec

case class YtTypeHolder(ytType: Option[TiType]) {
  def supportsSearchByName: Boolean = {
    ytType.exists(t => t.isStruct || (t.isVariant && t.asVariant().getUnderlying.isStruct))
  }

  def supportsSearchByIndex: Boolean = {
    ytType.exists(t => t.isStruct || t.isTuple || t.isVariant)
  }

  def getByName(name: String): YtTypeHolder = ytType match {
    case Some(t) if supportsSearchByName =>
      if (t.isVariant) {
        getMemberByName(t.asVariant().getUnderlying.asStruct(), name)
      } else {
        getMemberByName(t.asStruct(), name)
      }
    case _ => empty
  }

  def getByIndex(index: Int): YtTypeHolder = ytType match {
    case Some(t) if supportsSearchByIndex =>
      if (t.isVariant) {
        val underlying = t.asVariant().getUnderlying
        if (underlying.isTuple) {
          getElementByIndex(underlying.asTuple(), index)
        } else {
          getMemberByIndex(underlying.asStruct(), index)
        }
      } else if (t.isTuple) {
        getElementByIndex(t.asTuple(), index)
      } else {
        getMemberByIndex(t.asStruct(), index)
      }
    case _ => empty
  }

  def getListItem: YtTypeHolder = ytType match {
    case Some(t) if t.isList => YtTypeHolder(t.asList().getItem)
    case _ => empty
  }

  def isTuple: Boolean = ytType.exists(_.isTuple)
  def isStruct: Boolean = ytType.exists(_.isStruct)
  def isVariant: Boolean = ytType.exists(_.isVariant)
  def isList: Boolean = ytType.exists(_.isList)
  def isDict: Boolean = ytType.exists(_.isDict)

  def getVariantInner(i: Int): YtTypeHolder = ytType match {
    case Some(t) if t.isVariant =>
      val underlying = t.asVariant().getUnderlying
      if (underlying.isTuple) {
        getElementByIndex(underlying.asTuple(), i)
      } else {
        getMemberByIndex(underlying.asStruct(), i)
      }
    case _ => empty
  }

  def getVariantInner(name: String): YtTypeHolder = ytType match {
    case Some(t) if t.isVariant =>
      val underlying = t.asVariant().getUnderlying
      if (underlying.isTuple) {
        getMemberByName(underlying.asStruct(), name)
      } else {
        empty
      }
    case _ => empty
  }

  def getStructInner(i: Int): YtTypeHolder = ytType match {
    case Some(t) if t.isStruct => getMemberByIndex(t.asStruct(), i)
    case _ => empty
  }

  def getStructInner(fName: String): YtTypeHolder = ytType match {
    case Some(t) if t.isStruct => getMemberByName(t.asStruct(), fName)
    case _ => empty
  }

  def getTupleInner(i: Int): YtTypeHolder = ytType match {
    case Some(t) if t.isTuple => getElementByIndex(t.asTuple(), i)
    case _ => empty
  }

  def getMapKey: YtTypeHolder = ytType match {
    case Some(t) if t.isDict => YtTypeHolder(t.asDict().getKey)
    case _ => empty
  }

  def getMapValue: YtTypeHolder = ytType match {
    case Some(t) if t.isDict => YtTypeHolder(t.asDict().getValue)
    case _ => empty
  }
}

object YtTypeHolder {
  def apply(ytType: TiType): YtTypeHolder = YtTypeHolder(Some(ytType))

  def apply(ytType: Option[TiType] = None): YtTypeHolder = new YtTypeHolder(ytType.map(skipOptional))

  @tailrec
  private def skipOptional(ytType: TiType): TiType = {
    if (ytType.isOptional) {
      skipOptional(ytType.asOptional().getItem)
    } else {
      ytType
    }
  }

  private def getMemberByName(struct: ru.yandex.type_info.StructType,
                              name: String): YtTypeHolder = {
    val opt = struct.getMembers.stream().filter(m => m.getName == name).findFirst()
    if (opt.isPresent) {
      YtTypeHolder(opt.get().getType)
    } else {
      empty
    }
  }

  private def getMemberByIndex(struct: ru.yandex.type_info.StructType,
                               index: Int): YtTypeHolder = {
    if (struct.getMembers.size() > index) {
      YtTypeHolder(struct.getMembers.get(index).getType)
    } else {
      empty
    }
  }

  private def getElementByIndex(tuple: ru.yandex.type_info.TupleType,
                                index: Int): YtTypeHolder = {
    if (tuple.getElements.size() > index) {
      YtTypeHolder(tuple.getElements.get(index))
    } else {
      empty
    }
  }

  val empty: YtTypeHolder = YtTypeHolder()
}
