package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.ScalaReflection.universe.Type
import org.apache.spark.sql.catalyst.ScalaReflectionDecorators.baseType
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, ObjectType}
import org.apache.spark.sql.yson.UInt64Long
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.ScalaReflection")
object ScalaReflectionDecorators {

  @DecoratedMethod
  private def serializerFor(inputObject: Expression,
                            tpe: `Type`,
                            walkedTypePath: WalkedTypePath,
                            seenTypeSet: Set[`Type`]): Expression = baseType(tpe) match {
    case _ if !inputObject.dataType.isInstanceOf[ObjectType] => inputObject
    case t if isSubtype(t, ScalaReflection.localTypeOf[UInt64Long]) => UInt64Long.createSerializer(inputObject)
    case _ => __serializerFor(inputObject, tpe, walkedTypePath, seenTypeSet)
  }


  private def __serializerFor(inputObject: Expression,
                              tpe: `Type`,
                              walkedTypePath: WalkedTypePath,
                              seenTypeSet: Set[`Type`]): Expression = ???

  @DecoratedMethod
  private def deserializerFor(tpe: `Type`,
                              path: Expression,
                              walkedTypePath: WalkedTypePath): Expression = baseType(tpe) match {
    case t if !dataTypeFor(t).isInstanceOf[ObjectType] => path
    case t if isSubtype (t, ScalaReflection.localTypeOf[UInt64Long]) => UInt64Long.createDeserializer (path)
    case _ => __deserializerFor (tpe, path, walkedTypePath)
  }

  private def __deserializerFor(tpe: `Type`, path: Expression, walkedTypePath: WalkedTypePath): Expression = ???

  private def baseType(tpe: `Type`): `Type` = ???
  private[catalyst] def isSubtype(tpe1: `Type`, tpe2: `Type`): Boolean = ???
  private def dataTypeFor(tpe: `Type`): DataType = ???
}
