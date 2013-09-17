package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroByte
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

object AvroByteIO extends AvroByteIO

trait AvroByteIO extends AvroTypeIO[Byte] {

  def avroType = AvroByte

  protected[scalavro] def asGeneric[B <: Byte: TypeTag](value: B): Byte = value

  def write[B <: Byte: TypeTag](value: B, encoder: BinaryEncoder) = {
    encoder writeInt value.toInt
    encoder.flush
  }

  def read(decoder: BinaryDecoder) = Try {
    decoder.readInt.toByte
  }

}