package com.gensler.scalavro
package types

import com.gensler.scalavro
import com.gensler.scalavro.types.primitive._
import com.gensler.scalavro.types.complex._
import com.gensler.scalavro.error._
import com.gensler.scalavro.JsonSchemaProtocol._
import com.gensler.scalavro.io.AvroTypeIO

import scala.collection.mutable
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
    resolvedSymbols: mutable.Set[String] = mutable.Set[String]()): spray.json.JsValue

  /**
    * Returns the the schema of this Avro type in its most compact form.  That
    * is, with all named types represented as their fully qualified name.
    */
  def compactSchema(): spray.json.JsValue = {
    val knownTypeNames = mutable.Set(dependentNamedTypes.map(_.fullyQualifiedName): _*)
    selfContainedSchema(knownTypeNames)
  }

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
    */
  def parsingCanonicalForm(): JsValue

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
  lazy val dependentNamedTypes: Seq[AvroNamedType[_]] = this.computeDependencies()

  /**
    * == Internal API ==
    *
    * Helper method for dependentNamedTypes to short-circuit infinite recursion
    * in case of cyclic type dependency graphs.
    */
  protected[scalavro] def computeDependencies(
    previouslyEncounteredTypes: Set[AvroType[_]] = Set[AvroType[_]]()): Seq[AvroNamedType[_]] = {

    if (previouslyEncounteredTypes contains this) Seq()
    else {
      val knownTypes = previouslyEncounteredTypes + this

      this match {
        case at: AvroPrimitiveType[_] => Seq()
        case at: AvroArray[_, _]      => at.itemType.computeDependencies(knownTypes)
        case at: AvroSet[_, _]        => at.itemType.computeDependencies(knownTypes)
        case at: AvroMap[_, _]        => at.itemType.computeDependencies(knownTypes)

        case at: AvroUnion[_, _] => {
          at.memberAvroTypes.foldLeft(Seq[AvroNamedType[_]]()) {
            (aggregate, memberType) => aggregate ++ memberType.computeDependencies(knownTypes)
          }
        }

        case at: AvroRecord[_] => {
          at +: at.fields.map { _.fieldType }.foldLeft(Seq[AvroNamedType[_]]()) {
            (aggregate, fieldType) => aggregate ++ fieldType.computeDependencies(knownTypes)
          }
        }

        case at: AvroNamedType[_] => Seq(at)
      }
    }
  }.distinct

  /**
    * Returns an `AvroTypeIO` instance for this AvroType.
    */
  lazy val io: AvroTypeIO[T] = AvroTypeIO.avroTypeToIO(this)

}

/**
  * Companion object for [[AvroType]].
  */
object AvroType {

  import com.gensler.scalavro.types.primitive._
  import com.gensler.scalavro.types.complex._
  import com.gensler.scalavro.util.ReflectionHelpers
  import com.gensler.scalavro.util.Union
  import com.gensler.scalavro.util.FixedData
  import scala.collection.immutable

  object Cache {

    // primitive type cache table
    private[this] val primitiveTypeCache: Map[Type, AvroType[_]] = Map(
      typeOf[Unit] -> AvroNull,
      typeOf[Boolean] -> AvroBoolean,
      typeOf[java.lang.Boolean] -> AvroJavaBoolean,
      typeOf[Seq[Byte]] -> AvroBytes, // TODO: handle arbitrary subclasses of Seq[Byte]
      typeOf[immutable.Seq[Byte]] -> AvroBytes, // TODO: handle arbitrary subclasses of Seq[Byte]
      typeOf[Array[Byte]] -> AvroByteArray,
      typeOf[Double] -> AvroDouble,
      typeOf[java.lang.Double] -> AvroJavaDouble,
      typeOf[Float] -> AvroFloat,
      typeOf[java.lang.Float] -> AvroJavaFloat,
      typeOf[Byte] -> AvroByte,
      typeOf[java.lang.Byte] -> AvroJavaByte,
      typeOf[Char] -> AvroChar,
      typeOf[java.lang.Character] -> AvroJavaCharacter,
      typeOf[Short] -> AvroShort,
      typeOf[java.lang.Short] -> AvroJavaShort,
      typeOf[Int] -> AvroInt,
      typeOf[java.lang.Integer] -> AvroJavaInteger,
      typeOf[Long] -> AvroLong,
      typeOf[java.lang.Long] -> AvroJavaLong,
      typeOf[String] -> AvroString,
      typeOf[scala.xml.Node] -> AvroXml
    )

    // complex type cache table, initially empty
    private[this] var complexTypeCache = Map[Type, AvroType[_]]()

    protected[scalavro] def resolve(tpe: Type): Option[AvroType[_]] =
      primitiveTypeCache.get(tpe) orElse {
        primitiveTypeCache.collectFirst {
          case (cacheTpe, at) if cacheTpe =:= tpe => at
        } orElse {
          complexTypeCache.get(tpe) orElse {
            complexTypeCache.collectFirst {
              case (cacheTpe, at) if cacheTpe =:= tpe => at
            }
          }
        }
      }

    protected[scalavro] def save(tpe: Type, avroType: AvroType[_]) {
      complexTypeCache = complexTypeCache + (tpe -> avroType)
    }

  }

  /**
    * Returns an `AvroType[T]` for the supplied type `T` if one is available
    * or throws an exception.
    */
  def apply[T: TypeTag]: AvroType[T] = fromType[T].get

  protected[types] def cyclicTypeDependencyException[T: TypeTag] {
    throw new CyclicTypeDependencyException(
      "A cyclic type dependency was detected while attempting to " +
        "synthesize an AvroType for  type [%s]" format typeOf[T]
    )
  }

  /**
    * Returns a `Success[AvroType[T]]` if an analogous AvroType is available
    * for the supplied type.
    */
  def fromType[T](implicit typeTag: TypeTag[T]): Try[AvroType[T]] = fromTypeHelper(typeTag)

  private[scalavro] def fromTypeHelper[T](
    implicit tt: TypeTag[T],
    processedTypes: Set[Type] = Set[Type]()): Try[AvroType[T]] = Try {

    if (processedTypes exists { _ =:= tt.tpe }) cyclicTypeDependencyException[T]

    val tpe = tt.tpe

    val avroType = Cache.resolve(tpe) match {

      // complex type cache hit
      case Some(cachedAvroType) => cachedAvroType

      // cache miss
      case None => {

        val newComplexType = {

          // sets
          if (tpe <:< typeOf[Set[_]])
            AvroSet.fromType(processedTypes)(tt.asInstanceOf[TypeTag[Set[_]]])

          // string-keyed maps
          else if (tpe <:< typeOf[Map[String, _]])
            AvroMap.fromType(processedTypes)(tt.asInstanceOf[TypeTag[Map[String, _]]])

          // sequences and arrays
          else if (tpe <:< typeOf[Seq[_]] ||
            tpe <:< typeOf[Array[_]]) AvroArray.fromType(processedTypes)(tt.asInstanceOf[TypeTag[_]])

          // Scala enumerations
          else if (tpe.baseClasses.head.owner == typeOf[Enumeration].typeSymbol)
            AvroEnum.fromType(processedTypes)(tt.asInstanceOf[TypeTag[Enumeration]])

          // Java enums
          else if (ReflectionHelpers.classLoaderMirror.runtimeClass(tpe.typeSymbol.asClass).isEnum)
            AvroJEnum.fromType(processedTypes)(tt)

          // fixed-length data
          else if (tpe <:< typeOf[FixedData])
            AvroFixed.fromType(processedTypes)(tt.asInstanceOf[TypeTag[FixedData]])

          //sealed trait enum
          else if (tpe.typeSymbol.asClass.isSealed
            && !tpe.typeSymbol.asClass.knownDirectSubclasses.isEmpty
            && tpe.typeSymbol.asClass.knownDirectSubclasses.forall(_.isModuleClass))
            AvroSealedTraitEnum.fromType(processedTypes)(tt)

          // case classes
          else if (tpe <:< typeOf[Product] && tpe.typeSymbol.asClass.isCaseClass)
            AvroRecord.fromType(processedTypes)(tt.asInstanceOf[TypeTag[Product]])

          // unions
          else if (tpe <:< typeOf[Either[_, _]] ||
            tpe <:< typeOf[Option[_]] ||
            tpe <:< typeOf[Union.not[_]] ||
            tpe <:< typeOf[Union[_]] ||
            tpe.typeSymbol.isClass) AvroUnion.fromType(processedTypes)(tt)

          else throw new IllegalArgumentException(
            "Unable to find or make an AvroType for the supplied type [%s]" format tpe
          )
        }

        // add the synthesized AvroType to the complex type cache table
        Cache.save(tpe, newComplexType)

        newComplexType
      }
    }

    avroType.asInstanceOf[AvroType[T]]
  }

}