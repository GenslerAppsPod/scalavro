package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroBoolean
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import java.io.{ InputStream, OutputStream }

object AvroBooleanIO extends AvroBooleanIO

trait AvroBooleanIO extends AvroTypeIO[Boolean] {

  def avroType = AvroBoolean

  final val trueByte = 1.toByte
  final val falseByte = 0.toByte

  protected[scalavro] def asGeneric[B <: Boolean: TypeTag](value: B): Boolean = value

  /**
    * a boolean is written as a single byte whose value is either 0 (false) or
    * 1 (true).
    */
  def write[B <: Boolean: TypeTag](value: B, encoder: BinaryEncoder) = {
    encoder writeBoolean value
    encoder.flush
  }

  def read(decoder: BinaryDecoder) = Try {
    decoder.readBoolean
  }

}