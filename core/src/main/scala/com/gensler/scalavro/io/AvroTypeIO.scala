package com.gensler.scalavro.io

import java.io.{ ByteArrayInputStream, InputStream, OutputStream }

import com.gensler.scalavro.error._
import com.gensler.scalavro.io.AvroTypeIO.{ ToSchema, ToStream }
import com.gensler.scalavro.types.{ AvroPrimitiveType, AvroType }
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.io.{ BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory }
import spray.json._

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

abstract class AvroTypeIO[T: TypeTag] {

  /**
    * Returns the corresponding AvroType to this AvroTypeIO wrapper.
    */
  def avroType: AvroType[T]

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  /**
    * Writes a serialized representation of the supplied object according to
    * the Avro specification for binary encoding.  Throws an
    * AvroSerializationException if writing is unsuccessful.
    *
    * Output buffering is dependent upon the supplied `OutputStream`.
    *
    * The caller is responsible for calling `flush`; this method
    * may not flush the target stream.
    */
  @throws[AvroSerializationException[_]]
  final def write[G <: T: TypeTag](obj: G, stream: OutputStream): Unit = {
    val encoder = EncoderFactory.get.directBinaryEncoder(stream, null)
    write(obj, encoder, mutable.Map[Any, Long](), true)
  }

  /**
    * Writes a serialized representation of the supplied object according to
    * the Avro specification for binary encoding.  Throws an
    * AvroSerializationException if writing is unsuccessful.
    *
    * Output buffering is dependent upon the supplied `BinaryEncoder`.
    *
    * The caller is responsible for calling `flush`; this method
    * may not flush the target stream.
    */
  @throws[AvroSerializationException[_]]
  protected[scalavro] def write[G <: T: TypeTag](
    obj: G,
    encoder: BinaryEncoder,
    references: mutable.Map[Any, Long],
    topLevel: Boolean): Unit

  /**
    * Attempts to create an object of type T by reading the required data from
    * the supplied binary stream.
    */
  @throws[AvroDeserializationException[_]]
  final def read[D, S](data: D, writerSchema: S)(implicit d: ToStream[D], s: ToSchema[S]): Try[T] = read(data, Option(writerSchema))

  @throws[AvroDeserializationException[_]]
  final def read[D, S](data: D, writerSchema: Option[S] = Option.empty[Schema])(implicit d: ToStream[D], s: ToSchema[S]): Try[T] = {
    Try {
      val decoder = DecoderFactory.get.directBinaryDecoder(d.stream(data), null)
      writerSchema.map { schema =>
        read(decoder, mutable.ArrayBuffer[Any](), true, s.schema(schema))
      } getOrElse read(decoder, mutable.ArrayBuffer[Any](), true)
    }
  }

  /**
    * Attempts to create an object of type T by reading the required data from
    * the supplied decoder.
    */
  @throws[AvroDeserializationException[_]]
  private[scalavro] def read(
    decoder: BinaryDecoder,
    references: mutable.ArrayBuffer[Any],
    topLevel: Boolean): T

  /**
    * Attempts to create an object of type T by reading the required data from
    * the supplied decoder.
    */
  @throws[AvroDeserializationException[_]]
  private[scalavro] def read(
    decoder: BinaryDecoder,
    references: mutable.ArrayBuffer[Any],
    topLevel: Boolean,
    writerSchema: Schema): T = read(decoder, references, topLevel)

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  /**
    * Returns a serialized representation of the supplied object according to
    * the Avro specification for JSON encoding.  Throws an
    * AvroSerializationException if writing is unsuccessful.
    */
  @throws[AvroSerializationException[_]]
  def writeJson[G <: T: TypeTag](obj: G): JsValue

  /**
    * Attempts to create an object of type T by reading the required data from
    * the supplied JsValue.
    */
  @throws[AvroDeserializationException[_]]
  def readJson(json: JsValue): Try[T]

  /**
    * Attempts to create an object of type T by reading the required data from
    * the supplied JSON String.
    */
  @throws[AvroDeserializationException[_]]
  final def readJson(jsonString: String): Try[T] = Try {
    readJson(jsonString.parseJson).get
  }

}

/**
  * Companion object for [[AvroTypeIO]]
  *
  * Contains conversions from any AvroType to a corresponding
  * AvroTypeIO capable of reading and writing.
  */
object AvroTypeIO {

  import com.gensler.scalavro.io.complex._
  import com.gensler.scalavro.io.primitive._
  import com.gensler.scalavro.types.complex._
  import com.gensler.scalavro.types.primitive._
  import com.gensler.scalavro.util.{ FixedData, Union }

  import scala.language.implicitConversions

  // primitive types
  private def avroTypeToIO[T](avroType: AvroPrimitiveType[T]): AvroTypeIO[T] =
    avroType match {
      case AvroBoolean       => AvroBooleanIO
      case AvroBytes         => AvroBytesIO
      case AvroByteArray     => AvroByteArrayIO
      case AvroDouble        => AvroDoubleIO
      case AvroFloat         => AvroFloatIO
      case AvroByte          => AvroByteIO
      case AvroChar          => AvroCharIO
      case AvroShort         => AvroShortIO
      case AvroInt           => AvroIntIO
      case AvroLong          => AvroLongIO
      case AvroNull          => AvroNullIO
      case AvroString        => AvroStringIO // Covers Scala & Java strings (same class internally)
      case AvroXml           => AvroXmlIO

      case AvroJavaBoolean   => AvroJavaBooleanIO
      //case AvroJavaBytes     => AvroJavaBytesIO
      case AvroJavaDouble    => AvroJavaDoubleIO
      case AvroJavaFloat     => AvroJavaFloatIO
      case AvroJavaByte      => AvroJavaByteIO
      case AvroJavaCharacter => AvroJavaCharacterIO
      case AvroJavaShort     => AvroJavaShortIO
      case AvroJavaInteger   => AvroJavaIntegerIO
      case AvroJavaLong      => AvroJavaLongIO
      //case AvroW3Xml         => AvroW3XmlIO
    }

  // complex types
  private def avroTypeToIO[T, S <: Seq[T]](array: AvroArray[T, S]): AvroArrayIO[T, S] = AvroArrayIO(array)
  private def avroTypeToIO[T](array: AvroJArray[T]): AvroJArrayIO[T] = AvroJArrayIO(array)
  private def avroTypeToIO[T, S <: Set[T]](set: AvroSet[T, S]): AvroSetIO[T, S] = AvroSetIO(set)
  private def avroTypeToIO[T <: Enumeration](enum: AvroEnum[T]): AvroEnumIO[T] = AvroEnumIO(enum)
  private def avroTypeToIO[T](enum: AvroJEnum[T]): AvroJEnumIO[T] = AvroJEnumIO(enum)
  private def avroTypeToIO[T](enum: AvroSealedTraitEnum[T]): AvroSealedTraitEnumIO[T] = AvroSealedTraitEnumIO(enum)
  private def avroTypeToIO[T <: FixedData](fixed: AvroFixed[T]): AvroFixedIO[T] = AvroFixedIO(fixed)(fixed.tag)
  private def avroTypeToIO[T, M <: Map[String, T]](map: AvroMap[T, M]): AvroMapIO[T, M] = AvroMapIO(map)
  private def avroTypeToIO[T](error: AvroError[T]): AvroRecordIO[T] = AvroRecordIO(error)
  private def avroTypeToIO[T](record: AvroRecord[T]): AvroRecordIO[T] = AvroRecordIO(record)
  private def avroTypeToIO[U <: Union.not[_], T](union: AvroUnion[U, T]): AvroUnionIO[U, T] = AvroUnionIO(union)(union.union.underlyingTag, union.tag)

  def avroTypeToIO[T: TypeTag](at: AvroType[T]): AvroTypeIO[T] = {
    at match {
      case t: AvroPrimitiveType[_]   => avroTypeToIO(t)
      case t: AvroArray[_, _]        => avroTypeToIO(t)
      case t: AvroJArray[_]          => avroTypeToIO(t)
      case t: AvroSet[_, _]          => avroTypeToIO(t)
      case t: AvroEnum[_]            => avroTypeToIO(t)
      case t: AvroJEnum[_]           => avroTypeToIO(t)
      case t: AvroSealedTraitEnum[_] => avroTypeToIO(t)
      case t: AvroFixed[_]           => avroTypeToIO(t)
      case t: AvroMap[_, _]          => avroTypeToIO(t)
      case t: AvroError[_]           => avroTypeToIO(t)
      case t: AvroRecord[_]          => avroTypeToIO(t)
      case t: AvroUnion[_, _]        => avroTypeToIO(t)
    }
  }.asInstanceOf[AvroTypeIO[T]]

  trait ToStream[-D] {
    def stream(d: D): InputStream
  }
  object ToStream {
    implicit object InputToStream extends ToStream[InputStream] {
      override def stream(d: InputStream): InputStream = d
    }
    implicit object BytesToStream extends ToStream[Array[Byte]] {
      override def stream(d: Array[Byte]): InputStream = new ByteArrayInputStream(d)
    }
  }

  trait ToSchema[S] {
    def schema(s: S): Schema
  }
  object ToSchema {
    implicit object StringToSchema extends ToSchema[String] {
      override def schema(s: String): Schema = new Parser().parse(s)
    }
    implicit object DefaultToSchema extends ToSchema[Schema] {
      override def schema(s: Schema): Schema = s
    }
  }

}