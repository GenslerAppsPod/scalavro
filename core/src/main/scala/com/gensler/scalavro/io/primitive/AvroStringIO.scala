package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroString
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.generic.GenericData
import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

object AvroStringIO extends AvroStringIO

trait AvroStringIO extends AvroTypeIO[String] {

  def avroType = AvroString

  protected[scalavro] def asGeneric[S <: String: TypeTag](value: S): String = value

  def write[S <: String: TypeTag](value: S, encoder: BinaryEncoder) = {
    encoder writeString value
    encoder.flush
  }

  def read(decoder: BinaryDecoder) = Try {
    decoder.readString
  }

}
