package com.gensler.scalavro.io.complex

import com.gensler.scalavro.error.{ AvroDeserializationException, AvroSerializationException }
import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.io.primitive.AvroLongIO
import com.gensler.scalavro.types.complex.AvroRecord
import com.gensler.scalavro.util.ReflectionHelpers
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericRecord, GenericDatumReader }
import org.apache.avro.io.{ Decoder, BinaryDecoder, BinaryEncoder }
import spray.json._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

case class AvroRecordIO[T](avroType: AvroRecord[T]) extends AvroTypeIO[T]()(avroType.tag) {

  implicit val tt: TypeTag[T] = avroType.tag

  import ReflectionHelpers.{ CaseClassFactory, ProductElementExtractor }

  protected[this] lazy val extractors: Map[String, ProductElementExtractor[T, _]] = {
    avroType.fields.map { field => field.name -> extractorFor(field) }.toMap
  }

  private def extractorFor[F](field: AvroRecord.Field[F]): ProductElementExtractor[T, F] = {
    implicit val ft: TypeTag[F] = field.fieldType.tag
    new ProductElementExtractor[T, F](field.name)
  }

  protected[this] lazy val factory = new CaseClassFactory[T]

  protected[this] lazy val fieldReaders: Seq[AvroTypeIO[_]] = avroType.fields.map { _.fieldType.io }

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  val UNION_INDEX_RECORD: Long = 0
  val UNION_INDEX_REFERENCE: Long = 1

  protected[scalavro] def write[R <: T: TypeTag](
    obj: R,
    encoder: BinaryEncoder,
    references: mutable.Map[Any, Long],
    topLevel: Boolean): Unit = {

    if (topLevel) {
      // write the object
      writeFieldValues(obj, encoder, references)
    }
    else {
      references.get(obj) match {
        case Some(id: Long) => { // the object has already been written
          // encode the union index
          AvroLongIO.write(UNION_INDEX_REFERENCE, encoder)
          // encode the reference id
          AvroLongIO.write(id, encoder, references, false)
        }

        case None => { // the object has not been written to the stream yet
          // encode the union index
          AvroLongIO.write(UNION_INDEX_RECORD, encoder)

          // write the object
          writeFieldValues(obj, encoder, references)

          // add the object to the reference map
          references += obj -> references.size

        }
      }
    }
  }

  protected[this] def writeFieldValues[R <: T: TypeTag](
    obj: R,
    encoder: BinaryEncoder,
    references: mutable.Map[Any, Long]): Unit = {

    // encode the object as field values
    for (field <- avroType.fields) {
      try {
        val value = extractors(field.name).extractFrom(obj).asInstanceOf[Any]
        val fieldTag = field.fieldType.tag.asInstanceOf[TypeTag[Any]]
        field.fieldType.io.asInstanceOf[AvroTypeIO[Any]].write(value, encoder, references, false)(fieldTag)
      }
      catch {
        case cause: Throwable => {
          throw new AvroSerializationException(
            obj,
            cause,
            "Could not extract a value for field [%s]" format field.name
          )
        }
      }
    }
  }
  override protected[scalavro] def read(
    decoder: BinaryDecoder,
    references: mutable.ArrayBuffer[Any],
    topLevel: Boolean,
    writerSchema: Schema): T = read(decoder, references, topLevel, () => readObject(decoder, references, writerSchema))

  override protected[scalavro] def read(
    decoder: BinaryDecoder,
    references: mutable.ArrayBuffer[Any],
    topLevel: Boolean): T = read(decoder, references, topLevel, () => readObject(decoder, references))

  private def read(
    decoder: BinaryDecoder,
    references: mutable.ArrayBuffer[Any],
    topLevel: Boolean,
    readObject: () => T): T = {

    if (topLevel) readObject()
    else {
      (AvroLongIO read decoder) match {
        case UNION_INDEX_REFERENCE => {
          val index = AvroLongIO read decoder
          references(index.toInt).asInstanceOf[T]
        }
        case UNION_INDEX_RECORD => readObject()
        case _                  => throw new AvroDeserializationException
      }
    }
  }

  private def readObject(decoder: BinaryDecoder, references: mutable.ArrayBuffer[Any]): T = {
    val args = new scala.collection.mutable.ArrayBuffer[Any](initialSize = avroType.fields.size)
    try {
      for (reader <- fieldReaders) args += reader.read(decoder, references, false)
      val result = factory buildWith args
      references append result
      result
    }
    catch {
      case cause: Throwable => throw new AvroDeserializationException[T](
        cause,
        "The object's arguments were: [%s]" format args.mkString(", ")
      )
    }
  }

  private def skipRecord(schema: Schema, in: Decoder) = {
    new GenericDatumReader[Any](schema).read(null, in)
    ()
  }
  private def readObject(decoder: BinaryDecoder, references: mutable.ArrayBuffer[Any], writerSchema: Schema): T = {
    try {
      val readers: Map[String, AvroTypeIO[_]] = avroType.fields.map(field => (field.name, field.fieldType.io)).toMap
      val values = writerSchema.getFields
        .map(field => (
          field.name(),
          readers.get(field.name())
          .map(_.read(decoder, references, false, field.schema))
          .getOrElse(skipRecord(field.schema, decoder))
        )
        )
        .toMap
      val args = avroType.fields.map(field => values
        .get(field.name)
        .getOrElse(field.default
          .getOrElse(throw new NoSuchElementException(s"missing value of field ${field.name}"))))

      val result = factory buildWith args
      references append result
      result
    }
    catch {
      case cause: Throwable =>
        throw new AvroDeserializationException[T](cause, cause.getMessage)
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  def writeJson[R <: T: TypeTag](obj: R) = {

    val fields = avroType.fields.map { field =>
      try {
        val value = extractors(field.name).extractFrom(obj).asInstanceOf[Any]
        val fieldTag = field.fieldType.tag.asInstanceOf[TypeTag[Any]]
        field.name -> field.fieldType.io.asInstanceOf[AvroTypeIO[Any]].writeJson(value)(fieldTag)
      }
      catch {
        case cause: Throwable => throw new AvroSerializationException(
          obj,
          cause,
          "Could not extract a value for field [%s]" format field.name
        )
      }
    }

    JsObject(fields.toSeq: _*)
  }

  def readJson(json: JsValue) = Try {
    json match {
      case JsObject(fields) => {

        val args = (fields.values zip fieldReaders).map {
          case (fieldJson, reader) => reader.readJson(fieldJson).get
        }

        factory buildWith args.toSeq
      }
      case _ => throw new AvroDeserializationException[T]
    }
  }

}
