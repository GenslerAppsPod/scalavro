package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.complex.AvroJEnum
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import com.gensler.scalavro.util.ReflectionHelpers

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{ GenericData, GenericEnumSymbol, GenericDatumWriter, GenericDatumReader }
import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

import java.io.{ InputStream, OutputStream }

case class AvroJEnumIO[E](avroType: AvroJEnum[E]) extends AvroTypeIO[E]()(avroType.tag) {

  protected lazy val avroSchema: Schema = (new Parser) parse avroType.selfContainedSchema().toString

  protected[scalavro] def asGeneric[T <: E: TypeTag](obj: T): GenericEnumSymbol =
    new GenericData.EnumSymbol(avroSchema, obj.toString)

  def write[T <: E: TypeTag](obj: T, encoder: BinaryEncoder) = {
    try {
      val datumWriter = new GenericDatumWriter[GenericEnumSymbol](avroSchema)
      datumWriter.write(asGeneric(obj), encoder)
      encoder.flush
    }
    catch {
      case cause: Throwable =>
        throw new AvroSerializationException(obj, cause)
    }
  }

  def read(decoder: BinaryDecoder) = Try {
    val datumReader = new GenericDatumReader[GenericEnumSymbol](avroSchema)
    datumReader.read(null, decoder) match {
      case genericEnumSymbol: GenericEnumSymbol => avroType.symbolMap.get(genericEnumSymbol.toString).get
      case _                                    => throw new AvroDeserializationException[E]()(avroType.tag)
    }
  }

}