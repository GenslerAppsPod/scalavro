package com.gensler.scalavro.types

import com.gensler.scalavro.JsonSchemaProtocol._

import scala.collection.immutable.ListMap
import scala.reflect.runtime.universe.TypeTag

import spray.json._

/**
  * Marker trait for "named types".  As of version 1.7.4 of the Avro
  * specification, the named types are `Record`, `Enum`, and `Fixed`.
  */
abstract class AvroNamedType[T: TypeTag] extends AvroComplexType[T] {

  def name(): String

  def namespace(): Option[String]

  final def fullyQualifiedName(): String = namespace.map { _ + "." + name } getOrElse name

  protected[scalavro] override def canonicalFormOrFullyQualifiedName(): spray.json.JsValue =
    this.fullyQualifiedName.toJson

  def fullyQualify(json: JsValue): JsValue = json match {
    case JsObject(fields) => new JsObject(ListMap(
      "name" -> fullyQualifiedName.toJson) ++
      (fields -- Seq("name", "namespace")
    ))
    case otherJsValue: JsValue => otherJsValue
  }

}
