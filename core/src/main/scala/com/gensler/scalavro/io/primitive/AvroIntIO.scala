package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroInt
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

object AvroIntIO extends AvroIntIO

trait AvroIntIO extends AvroTypeIO[Int] {

  def avroType = AvroInt

  protected[scalavro] def asGeneric[I <: Int: TypeTag](value: I): Int = value

  def write[I <: Int: TypeTag](value: I, encoder: BinaryEncoder) = {
    encoder writeInt value
    encoder.flush
  }

  def read(decoder: BinaryDecoder) = Try {
    decoder.readInt
  }

}