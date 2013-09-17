package com.gensler.scalavro
package types

import com.gensler.scalavro
import com.gensler.scalavro.types.primitive._
import com.gensler.scalavro.types.complex._
import com.gensler.scalavro.error._
import com.gensler.scalavro.JsonSchemaProtocol._
import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.util.Logging

import scala.util.{ Try, Success, Failure }
import scala.language.existentials
import scala.reflect.runtime.universe._

import spray.json._

import java.io.DataOutputStream
import java.security.MessageDigest

/**
  * Abstract parent class of all Avro types.  An [[AvroType]] wraps a
  * corresponding type from the Scala type system.
  *
  * To obtain an `AvroType` instance use the `apply` or `fromType` method,
  * defined on the `AvroType` companion object.
  *
  * {{{
  *   import com.gensler.scalavro.types.AvroType
  *
  *   val avroString = AvroType[String]
  *   avroString.schema
  * }}}
  */
abstract class AvroType[T: TypeTag] extends JsonSchemifiable with CanonicalForm {

  final val tag: TypeTag[T] = typeTag[T]

  /**
    * The corresponding Scala type for this Avro type.
    */
  type scalaType = T

  /**
    * Returns the Avro type name for this schema.
    */
  def typeName(): String

  /**
    * Returns true if this represents a primitive Avro type.
    */
  def isPrimitive(): Boolean

  /**
    * Returns the JSON representation of this Avro type schema.
    */
  def schema(): spray.json.JsValue

  /**
    * Returns the fully self-describing JSON representation of this Avro type
    * schema.
    */
  def selfContainedSchema(
    resolvedSymbols: scala.collection.mutable.Set[String] = scala.collection.mutable.Set[String]()): spray.json.JsValue

  /**
    * Returns the schema name if this is an instance of [[AvroNamedType]], or
    * the expanded schema otherwise.
    */
  def schemaOrName(): spray.json.JsValue =
    if (typeOf[this.type] <:< typeOf[AvroNamedType[_]])
      this.asInstanceOf[AvroNamedType[_]].name.toJson
    else this.schema

  /**
    * == Internal API ==
    *
    * Returns the fully qualified schema name if this is an instance of
    * [[AvroNamedType]], or the parsing canonical form of this type schema
    * otherwise.
    */
  protected[scalavro] def canonicalFormOrFullyQualifiedName(): spray.json.JsValue =
    this.parsingCanonicalForm

  /**
    * Returns the JSON schema for this type in "parsing canonical form".
    *
    * _X_ [PRIMITIVES] Convert primitive schemas to their simple form (e.g.,
    *     int instead of {"type":"int"}).
    *
    * _X_ [FULLNAMES] Replace short names with fullnames, using applicable
    *     namespaces to do so. Then eliminate namespace attributes, which are
    *     now redundant.
    *
    * _X_ [STRIP] Keep only attributes that are relevant to parsing data, which
    *     are: type, name, fields, symbols, items, values, size. Strip all
    *     others (e.g., doc and aliases).
    *
    * _X_ [ORDER] Order the appearance of fields of JSON objects as follows:
    *     name, type, fields, symbols, items, values, size. For example, if an
    *     object has type, name, and size fields, then the name field should
    *     appear first, followed by the type and then the size fields.
    *
    * _X_ [INTEGERS] Eliminate quotes around and any leading zeros in front of
    *     JSON integer literals (which appear in the size attributes of fixed
    *     schemas).
    *
    * _X_ [WHITESPACE] Eliminate all whitespace in JSON outside of string
    *     literals.
    */
  def parsingCanonicalForm(): JsValue = schema

  /**
    * _X_ [STRINGS] For all JSON string literals in the schema text, replace
    *     any escaped characters (e.g., \\uXXXX escapes) with their UTF-8
    *     equivalents.
    */
  def writeCanonicalForm(os: java.io.OutputStream) {
    new DataOutputStream(os) writeUTF parsingCanonicalForm.toString
  }

  override def toString(): String = {
    val className = getClass.getSimpleName
    if (className endsWith "$") className.dropRight(1) else className
  }

  /**
    * Returns the result of computing MD5 over this type's parsing canonical
    * form.
    */
  final lazy val fingerprint: Array[Byte] = {
    val MD5 = MessageDigest.getInstance("MD5")
    MD5.digest(parsingCanonicalForm.toString.getBytes)
  }

  /**
    * Returns true if this type depends upon the supplied type.
    */
  def dependsOn(thatType: AvroType[_]): Boolean

  /**
    * Returns the sequence of named types that are required to fully
    * specify this AvroType, including recursive/transitive
    * type dependencies.
    */
  lazy val dependentNamedTypes: Seq[AvroNamedType[_]] = {
    this match {
      case at: AvroPrimitiveType[_] => Seq()
      case at: AvroArray[_, _]      => at.itemType.dependentNamedTypes
      case at: AvroSet[_, _]        => at.itemType.dependentNamedTypes
      case at: AvroMap[_, _]        => at.itemType.dependentNamedTypes

      case at: AvroUnion[_, _] => {
        at.memberAvroTypes.foldLeft(Seq[AvroNamedType[_]]()) {
          (aggregate, memberType) => aggregate ++ memberType.dependentNamedTypes
        }
      }

      case at: AvroRecord[_] => {
        at +: at.fields.map { _.fieldType }.foldLeft(Seq[AvroNamedType[_]]()) {
          (aggregate, fieldType) => aggregate ++ fieldType.dependentNamedTypes
        }
      }

      case at: AvroNamedType[_] => Seq(at)
    }
  }.distinct

  /**
    * Returns an [[AvroTypeIO]] instance for this AvroType.
    */
  lazy val io: AvroTypeIO[T] = AvroTypeIO.avroTypeToIO(this)

}

/**
  * Companion object for [[AvroType]].
  */
object AvroType extends Logging {

  import com.gensler.scalavro.types.primitive._
  import com.gensler.scalavro.types.complex._
  import com.gensler.scalavro.util.ReflectionHelpers
  import com.gensler.scalavro.util.Union
  import com.gensler.scalavro.util.FixedData
  import scala.collection.immutable
  import scala.collection.immutable.ListMap
  import java.util.concurrent.atomic.AtomicReference

  // primitive type cache table
  private val primitiveTypeCache: ListMap[Type, AvroType[_]] = ListMap(
    typeOf[Unit] -> AvroNull,
    typeOf[Boolean] -> AvroBoolean,
    typeOf[Seq[Byte]] -> AvroBytes, // TODO: handle arbitrary subclasses of Seq[Byte]
    typeOf[immutable.Seq[Byte]] -> AvroBytes, // TODO: handle arbitrary subclasses of Seq[Byte]
    typeOf[Double] -> AvroDouble,
    typeOf[Float] -> AvroFloat,
    typeOf[Byte] -> AvroByte,
    typeOf[Char] -> AvroChar,
    typeOf[Short] -> AvroShort,
    typeOf[Int] -> AvroInt,
    typeOf[Long] -> AvroLong,
    typeOf[String] -> AvroString
  )

  // complex type cache table, initially empty
  private[scalavro] val complexTypeCache =
    new AtomicReference[ListMap[Type, AvroType[_]]](
      ListMap[Type, AvroType[_]]()
    )

  /**
    * Returns an `AvroType[T]` for the supplied type `T` if one is available
    * or throws an exception.
    * `
    */
  def apply[T: TypeTag]: AvroType[T] = fromType[T].get

  /**
    * Returns a `Success[AvroType[T]]` if an analogous AvroType is available
    * for the supplied type.
    */
  def fromType[T](implicit typeTag: TypeTag[T]): Try[AvroType[T]] = fromTypeHelper(typeTag)

  private[scalavro] def fromTypeHelper[T](
    implicit tt: TypeTag[T],
    processedTypes: Set[Type] = Set[Type]()): Try[AvroType[T]] = Try {

    def cyclicTypeDependencyException() {
      throw new CyclicTypeDependencyException(
        "A cyclic type dependency was detected while attempting to " +
          "synthesize an AvroType for  type [%s]" format tt.tpe
      )
    }

    if (processedTypes exists { _ =:= tt.tpe }) cyclicTypeDependencyException()

    val tpe = tt.tpe

    val avroType = primitiveTypeCache.collectFirst { case (cacheTpe, at) if cacheTpe =:= tpe => at } match {

      // primitive type cache hit
      case Some(primitive) => primitive

      // primitive type cache miss
      case None => complexTypeCache.get.collectFirst { case (cacheTpe, at) if cacheTpe =:= tpe => at } match {

        // complex type cache hit
        case Some(complex) => complex

        // cache miss
        case None => {

          val newComplexType = {

            // sets
            if (tpe <:< typeOf[Set[_]]) {

              // Traverse up to the Seq supertype and get the type of items
              val setSuperSymbol = tpe.baseClasses.map(_.asType).find { bcSymbol =>
                bcSymbol == typeOf[Set[_]].typeSymbol
              }

              val itemType = setSuperSymbol.map(_.typeParams(0).asType.toTypeIn(tpe)).get

              if (processedTypes.exists { _ =:= itemType })
                cyclicTypeDependencyException()

              def makeSet[I](itemTag: TypeTag[I]) = {
                new AvroSet()(itemTag, tt.asInstanceOf[TypeTag[Set[I]]])
              }

              if (!ReflectionHelpers.companionVarargsApply[T].isDefined) {
                throw new IllegalArgumentException(
                  "Set subclasses must have a companion object with a public varargs " +
                    "apply method, but no such method was found for type [%s].".format(tpe)
                )
              }

              makeSet(ReflectionHelpers.tagForType(itemType))
            }

            // string-keyed maps
            else if (tpe <:< typeOf[Map[String, _]]) {

              // Traverse up to the Map supertype and get the type of items
              val mapSuperSymbol = tpe.baseClasses.map(_.asType).find { bcSymbol =>
                bcSymbol == typeOf[Map[_, _]].typeSymbol
              }

              val itemType = mapSuperSymbol.map(_.typeParams(1).asType.toTypeIn(tpe)).get

              if (processedTypes.exists { _ =:= itemType })
                cyclicTypeDependencyException()

              def makeMap[I](itemTag: TypeTag[I]) = {
                new AvroMap()(itemTag, tt.asInstanceOf[TypeTag[Map[String, I]]])
              }

              if (!ReflectionHelpers.companionVarargsApply[T].isDefined) {
                throw new IllegalArgumentException(
                  "String-keyed map subclasses must have a companion object with a public varargs " +
                    "apply method, but no such method was found for type [%s].".format(tpe)
                )
              }

              makeMap(ReflectionHelpers.tagForType(itemType))
            }

            // sequences
            else if (tpe <:< typeOf[Seq[_]]) {

              // Traverse up to the Seq supertype and get the type of items
              val seqSuperSymbol = tpe.baseClasses.map(_.asType).find { bcSymbol =>
                bcSymbol == typeOf[Seq[_]].typeSymbol
              }

              val itemType = seqSuperSymbol.map(_.typeParams(0).asType.toTypeIn(tpe)).get

              if (processedTypes.exists { _ =:= itemType })
                cyclicTypeDependencyException()

              def makeArray[I](itemTag: TypeTag[I]) = {
                new AvroArray()(itemTag, tt.asInstanceOf[TypeTag[Seq[I]]])
              }

              if (!ReflectionHelpers.companionVarargsApply[T].isDefined) {
                throw new IllegalArgumentException(
                  "Sequence subclasses must have a companion object with a public varargs " +
                    "apply method, but no such method was found for type [%s].".format(tpe)
                )
              }

              makeArray(ReflectionHelpers.tagForType(itemType))
            }

            // Scala enumerations
            else if (tpe.baseClasses.head.owner == typeOf[Enumeration].typeSymbol) {
              tpe match {
                case TypeRef(prefix, symbol, _) =>

                  val enumTypeTag = ReflectionHelpers.enumForValue(tt.asInstanceOf[TypeTag[_ <: Enumeration#Value]])

                  new AvroEnum(
                    name = symbol.name.toString,
                    symbols = ReflectionHelpers.symbolsOf(enumTypeTag),
                    namespace = Some(prefix.toString stripSuffix ".type")
                  )(enumTypeTag)
              }
            }

            // Java enums
            else if (ReflectionHelpers.classLoaderMirror.runtimeClass(tpe.typeSymbol.asClass).isEnum) {
              val enumClass = ReflectionHelpers.classLoaderMirror.runtimeClass(tpe.typeSymbol.asClass)
              new AvroJEnum[T](
                name = enumClass.getSimpleName,
                symbols = enumClass.getEnumConstants.map(_.toString),
                namespace = Some(enumClass.getPackage.getName)
              )
            }

            // fixed-length data
            else if (tpe <:< typeOf[FixedData]) {
              FixedData.lengthAnnotationInstance(tpe.typeSymbol.asClass) match {
                case Some(FixedData.Length(dataLength)) => {
                  val TypeRef(prefix, symbol, _) = tpe

                  if (tpe.typeSymbol.asClass.typeParams.nonEmpty) {
                    throw new IllegalArgumentException(
                      "FixedData classes with type parameters are not supported"
                    )
                  }

                  if (!ReflectionHelpers.singleArgumentConstructor[T, immutable.Seq[Byte]].isDefined) {
                    throw new IllegalArgumentException(
                      "FixedData classes must define a public single-argument constructor taking a Seq[Byte]"
                    )
                  }

                  new AvroFixed(
                    name = symbol.name.toString,
                    size = dataLength,
                    namespace = Some(prefix.toString stripSuffix ".type")
                  )(tt.asInstanceOf[TypeTag[FixedData]])
                }
                case None => throw new IllegalArgumentException(
                  "FixedData classes must be decorated with a FixedData.Length annotation"
                )
              }
            }

            // case classes
            else if (tpe <:< typeOf[Product] &&
              tpe.typeSymbol.asClass.isCaseClass &&
              tpe.typeSymbol.asClass.typeParams.isEmpty) {
              tpe match {
                case TypeRef(prefix, symbol, _) =>
                  new AvroRecord[T](
                    name = symbol.name.toString,
                    fields = ReflectionHelpers.caseClassParamsOf[T].toSeq map {
                      case (name, tag) => {
                        val fieldType = fromTypeHelper(tag, (processedTypes + tt.tpe)).get
                        AvroRecord.Field(name, fieldType)
                      }
                    },
                    namespace = Some(prefix.toString stripSuffix ".type")
                  )
              }
            }

            // binary unions via scala.Either[A, B]
            else if (tpe <:< typeOf[Either[_, _]]) tpe match {
              case TypeRef(_, _, List(left, right)) => {
                if (processedTypes.exists { pt => pt =:= left || pt =:= right })
                  cyclicTypeDependencyException()

                new AvroUnion(
                  Union.combine(
                    Union.unary(ReflectionHelpers.tagForType(left)).underlyingConjunctionTag,
                    ReflectionHelpers.tagForType(right)
                  ),
                  tt
                )
              }
            }

            // binary unions via scala.Option[T]
            else if (tpe <:< typeOf[Option[_]]) tpe match {
              case TypeRef(_, _, List(innerType)) => {

                if (processedTypes.exists { _ =:= innerType })
                  cyclicTypeDependencyException()

                new AvroUnion(
                  Union.combine(
                    Union.unary(typeTag[Unit]).underlyingConjunctionTag,
                    ReflectionHelpers.tagForType(innerType)
                  ),
                  tt
                )
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

            // abstract super types of concrete avro-typable types
            else if (tpe.typeSymbol.isClass) {
              // last-ditch attempt: union of avro-typeable subtypes of T
              import ReflectionHelpers._

              val subTypeTags = typeableSubTypesOf[T].filter { subTypeTag =>
                fromTypeHelper(
                  subTypeTag,
                  processedTypes + tpe
                ).toOption.isDefined
              }

              if (subTypeTags.nonEmpty) {
                var u = Union.unary(subTypeTags.head)

                subTypeTags.tail.foreach { subTypeTag =>
                  u = Union.combine(
                    u.underlyingConjunctionTag.asInstanceOf[TypeTag[Any]],
                    subTypeTag
                  )
                }

                new AvroUnion(u, tt)
              } // other types are not handled

              else throw new IllegalArgumentException(
                "Unable to find or make an AvroType for the supplied type [%s]" format tpe
              )
            }

            else throw new IllegalArgumentException(
              "Unable to find or make an AvroType for the supplied type [%s]" format tpe
            )
          }

          // add the synthesized AvroType to the complex type cache table
          var tries = 0
          var cacheUpdated = false
          while (!cacheUpdated && tries < 3) {
            val currentCache = complexTypeCache.get
            cacheUpdated = complexTypeCache.compareAndSet(
              currentCache,
              currentCache + (tpe -> newComplexType)
            )
            tries += 1
          }

          newComplexType
        }
      }
    }

    avroType.asInstanceOf[AvroType[T]]
  }

}