package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.AvroPrimitiveType
import com.gensler.scalavro.types.complex.AvroRecord
import com.gensler.scalavro.error.{AvroSerializationException, AvroDeserializationException}
import com.gensler.scalavro.util.ReflectionHelpers

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericRecord, GenericData, GenericDatumWriter, GenericDatumReader}
import org.apache.avro.io.{EncoderFactory, DecoderFactory}
import com.gensler.scalavro.io.AvroTypeIO.Implicits._

import scala.util.{Try, Success, Failure}
import scala.reflect.runtime.universe.{TypeTag, typeTag}

import java.io.{InputStream, OutputStream}

case class AvroRecordIO[T](avroType: AvroRecord[T]) extends AvroTypeIO[T] {

  protected lazy val avroSchema: Schema = (new Parser) parse avroType.selfContainedSchema().toString
 
  /**
    * Returns the [[org.apache.avro.generic.GenericRecord]] representation of
    * this AvroRecord.
    */
  protected[scalavro] def asGeneric[R <: T : TypeTag](obj: R): GenericRecord = {
    val record = new GenericData.Record(avroSchema)
    avroType.fields.foreach { field =>
      ReflectionHelpers.productElement(obj, field.name)(typeTag[R], field.fieldType.tag) foreach { value =>
        val io = avroTypeToIO(field.fieldType)
        record.put(field.name, io asGeneric value)
      }
    }
    return record
  }

  protected[scalavro] def fromGeneric(obj: Any): T = obj match {
    case record: GenericRecord => {
      ReflectionHelpers.instantiateCaseClassWith(
        avroType.fields map { field =>
          field.fieldType fromGeneric record.get(field.name)
        }
      )(avroType.tag).get
    }
    case _ => throw new AvroDeserializationException()(avroType.tag)
  }

  /**
    * Writes a binary representation of the supplied object to the supplied
    * stream.
    */
  def write[R <: T : TypeTag](obj: R, stream: OutputStream) {
    try {
      val datumWriter = new GenericDatumWriter[GenericRecord](avroSchema)
      val encoder = EncoderFactory.get.binaryEncoder(stream, null)
      datumWriter.write(asGeneric(obj), encoder)
      encoder.flush
    }
    catch { case cause: Throwable => 
      throw new AvroSerializationException(obj, cause)
    }
  }

  /**
    * Reads a binary representation of the underlying Scala type from the
    * supplied stream.
    */
  def read(stream: InputStream) = Try {
    val datumReader = new GenericDatumReader[GenericRecord](avroSchema)
    val decoder = DecoderFactory.get.directBinaryDecoder(stream, null)
    this fromGeneric datumReader.read(null, decoder)
  }

}