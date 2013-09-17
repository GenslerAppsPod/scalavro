package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.AvroType
import com.gensler.scalavro.types.AvroPrimitiveType
import com.gensler.scalavro.types.complex.AvroRecord
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import com.gensler.scalavro.util.ReflectionHelpers

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{ GenericRecord, GenericData, GenericDatumWriter }
import org.apache.avro.io.EncoderFactory

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.{ TypeTag, typeTag }

import java.io.{ InputStream, OutputStream }

case class AvroRecordIO[T](avroType: AvroRecord[T]) extends AvroTypeIO[T]()(avroType.tag) {

  /**
    * Writes a binary representation of the supplied object to the supplied
    * stream.
    */
  def write[R <: T: TypeTag](obj: R, encoder: BinaryEncoder) {
    try {
      avroType.fields.foreach { field =>
        writeFieldValue(field.fieldType.tag)
        def writeFieldValue[V: TypeTag] = {
          ReflectionHelpers.productElement[R, V](obj, field.name) match {
            case Some(value) => {
              field.fieldType.asInstanceOf[AvroType[V]].io.write(value, encoder)
            }
            case None => throw new RuntimeException(
              "Could not extract a value for field [%s]" format field.name
            )
          }
        }
      }
    }
    catch {
      case cause: Throwable =>
        throw new AvroSerializationException(obj, cause)
    }
  }

  /**
    * Reads a binary representation of the underlying Scala type from the
    * supplied stream.
    */
  def read(decoder: BinaryDecoder) = Try {
    val args = avroType.fields map { field => field.fieldType.io.read(decoder).get }
    ReflectionHelpers.instantiateCaseClassWith(args)(avroType.tag).get
  }

}