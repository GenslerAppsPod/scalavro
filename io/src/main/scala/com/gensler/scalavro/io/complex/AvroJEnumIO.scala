package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.complex.AvroJEnum
import com.gensler.scalavro.error.{AvroSerializationException, AvroDeserializationException}
import com.gensler.scalavro.util.ReflectionHelpers

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericEnumSymbol, GenericDatumWriter, GenericDatumReader}
import org.apache.avro.io.{EncoderFactory, DecoderFactory}

import scala.util.{Try, Success, Failure}
import scala.reflect.runtime.universe.TypeTag

import java.io.{InputStream, OutputStream}

case class AvroJEnumIO[E](avroType: AvroJEnum[E]) extends AvroTypeIO[E] {

  protected lazy val avroSchema: Schema = (new Parser) parse avroType.selfContainedSchema().toString

  protected[scalavro] def asGeneric[T <: E : TypeTag](obj: T): GenericEnumSymbol =
    new GenericData.EnumSymbol(avroSchema, obj.toString)

  def fromGeneric(obj: Any): E = obj match {
    case genericEnumSymbol: GenericEnumSymbol => avroType.symbolMap.get(genericEnumSymbol.toString).get
    case _ => throw new AvroDeserializationException[E]()(avroType.tag)
  }

  def write[T <: E : TypeTag](obj: T, stream: OutputStream) = {
    try {
      val datumWriter = new GenericDatumWriter[GenericEnumSymbol](avroSchema)
      val encoder = EncoderFactory.get.binaryEncoder(stream, null)
      datumWriter.write(asGeneric(obj), encoder)
      encoder.flush
    }
    catch { case cause: Throwable => 
      throw new AvroSerializationException(obj, cause)
    }
  }

  def read(stream: InputStream) = Try {
    val datumReader = new GenericDatumReader[GenericEnumSymbol](avroSchema)
    val decoder = DecoderFactory.get.directBinaryDecoder(stream, null)
    this fromGeneric datumReader.read(null, decoder)
  }

}