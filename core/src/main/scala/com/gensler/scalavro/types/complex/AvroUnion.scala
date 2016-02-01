package com.gensler.scalavro.types.complex

import com.gensler.scalavro.JsonSchemaProtocol._
import com.gensler.scalavro.types.{ AvroComplexType, AvroNullablePrimitiveType, AvroType }
import com.gensler.scalavro.util.{ ReflectionHelpers, Union }
import com.gensler.scalavro.util.Union.union
import spray.json._

import scala.collection.mutable
import scala.reflect.runtime.universe._

/**
  * Represents a mapping from a source Scala type to a corresponding
  * Avro type.
  *
  * Valid source types are many, including scala.Option, scala.Either,
  * com.gensler.scalavro.util.Union, or any abstract type with known concrete
  * case class subclasses.
  */
class AvroUnion[U <: Union.not[_]: TypeTag, T](
    val union: Union[U],
    val originalType: TypeTag[T]) extends AvroComplexType()(originalType) {

  val memberAvroTypes = union.typeMembers.map {
    tpe => AvroType.fromType(ReflectionHelpers tagForType tpe).get
  }

  val typeName = "union"

  def selfContainedSchema(
    resolvedSymbols: mutable.Set[String] = mutable.Set[String]()) = {
    memberAvroTypes.map {
      case at: AvroNullablePrimitiveType[_] => JsString(at.typeName())
      case at                               => selfContainedSchemaOrFullyQualifiedName(at, resolvedSymbols)
    }.toJson
  }

}

class AvroReferenceUnion[U <: Union.not[_]: TypeTag, T](
    union: Union[U],
    originalType: TypeTag[T]) extends AvroUnion[U, T](union, originalType) {

  override def selfContainedSchema(resolvedSymbols: mutable.Set[String] = mutable.Set[String]()) = memberAvroTypes.map {
    _ match {
      case recordType: AvroRecord[_] => recordType.selfContainedSchema(resolvedSymbols)
      case at: AvroType[_]           => selfContainedSchemaOrFullyQualifiedName(at, resolvedSymbols)
    }
  }.toJson

}

object AvroUnion {

  import com.gensler.scalavro.Reference
  import com.gensler.scalavro.util.ReflectionHelpers

  def referenceUnionFor[T](recordType: AvroRecord[T]): AvroReferenceUnion[_, _] = {
    implicit val recordTypeTag: TypeTag[T] = recordType.tag

    new AvroReferenceUnion(
      new Union[union[T]#or[Reference]],
      typeTag[Either[T, Reference]]
    )
  }

  private[types] def fromType[T: TypeTag](processedTypes: Set[Type]): AvroUnion[_, T] = {
    val tt = typeTag[T]
    val tpe = tt.tpe

    // binary unions via scala.Either[A, B]
    if (tpe <:< typeOf[Either[_, _]]) {
      tpe match {
        case TypeRef(_, _, List(left, right)) => {
          if (processedTypes.exists { pt => pt =:= left || pt =:= right })
            AvroType.cyclicTypeDependencyException[T]

          new AvroUnion(
            Union.combine(
              Union.unary(ReflectionHelpers.tagForType(left)).underlyingConjunctionTag,
              ReflectionHelpers.tagForType(right)
            ),
            tt
          )
        }
      }
    }

    // binary unions via scala.Option[T]
    else if (tpe <:< typeOf[Option[_]]) {
      tpe match {
        case TypeRef(_, _, List(innerType)) => {

          if (processedTypes.exists { _ =:= innerType })
            AvroType.cyclicTypeDependencyException[T]

          new AvroUnion(
            Union.combine(
              Union.unary(typeTag[Unit]).underlyingConjunctionTag,
              ReflectionHelpers.tagForType(innerType)
            ),
            tt
          )
        }
      }
    }

    // N-ary unions
    else if (tpe <:< typeOf[Union.not[_]]) {
      new AvroUnion(new Union()(tt.asInstanceOf[TypeTag[Union.not[_]]]), tt)
    }

    // N-ary unions
    else if (tpe <:< typeOf[Union[_]]) {
      val TypeRef(_, _, List(notType)) = tpe
      val notTypeTag = ReflectionHelpers.tagForType(notType).asInstanceOf[TypeTag[Union.not[_]]]
      new AvroUnion(new Union()(notTypeTag), tt)
    }

    // super types of concrete avro-typeable types
    else if (tpe.typeSymbol.isClass) {
      // last-ditch attempt: union of avro-typeable subtypes of T
      val extendedProcessedTypes = processedTypes + tpe

      val subTypeTags = ReflectionHelpers.typeableSubTypesOf[T].filter { subTypeTag =>
        AvroType.fromTypeHelper(
          subTypeTag,
          extendedProcessedTypes
        ).toOption.isDefined
      }.sortBy { subTypeTag => subTypeTag.tpe.typeSymbol.fullName }

      if (subTypeTags.nonEmpty) {
        import scala.language.existentials
        var u = Union.unary(subTypeTags.head)

        subTypeTags.tail.foreach { subTypeTag =>
          u = Union.combine(
            u.underlyingConjunctionTag,
            subTypeTag
          )
        }

        new AvroUnion(u, tt)
      }
      else throw new IllegalArgumentException(
        "Could not find any avro-typeable sub types of [%s]" format tpe
      )
    }

    else throw new IllegalArgumentException("""
      |Could not create a union from type [%s]
      |Union types may only be synthesized from:"
      | - Either[_, _]
      | - Option[_]
      | - com.gensler.scalavro.util.Union.not
      | - com.gensler.scalavro.util.Union
      | - Super types of concrete avro-typeable types
    """.format(tpe).stripMargin)
  }

}
