package com.gensler.scalavro.types.complex

import com.gensler.scalavro.types.{AvroType, AvroComplexType}
import com.gensler.scalavro.types.primitive.AvroNull
import com.gensler.scalavro.JsonSchemaProtocol._
import com.gensler.scalavro.util.ReflectionHelpers

import com.gensler.scalavro.util.Union

import scala.reflect.runtime.universe._
import scala.util.Success

import spray.json._

class AvroUnion[U <: Union.not[_]: TypeTag](val union: Union[U]) extends AvroComplexType[U] {

  val memberAvroTypes = union.typeMembers.map {
    tpe => AvroType.fromType(ReflectionHelpers tagForType tpe).get
  }

  val typeName = "union"

  def schema() = memberAvroTypes.map { _.schema }.toJson

  def selfContainedSchema(
    resolvedSymbols: scala.collection.mutable.Set[String] = scala.collection.mutable.Set[String]()
  ) = memberAvroTypes.map { at =>
    selfContainedSchemaOrFullyQualifiedName(at, resolvedSymbols)
  }.toJson

  override def parsingCanonicalForm(): JsValue =
    memberAvroTypes.map { _.canonicalFormOrFullyQualifiedName }.toJson

  def dependsOn(thatType: AvroType[_]) = memberAvroTypes.exists { _ dependsOn thatType }

}
