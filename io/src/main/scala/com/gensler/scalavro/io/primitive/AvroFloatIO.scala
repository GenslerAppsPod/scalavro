package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroFloat
import com.gensler.scalavro.error.{AvroSerializationException, AvroDeserializationException}

import org.apache.avro.io.{EncoderFactory, DecoderFactory}

import scala.util.{Try, Success, Failure}
import scala.reflect.runtime.universe.TypeTag

import java.io.{InputStream, OutputStream}

object AvroFloatIO extends AvroFloatIO

trait AvroFloatIO extends AvroTypeIO[Float] {

  def avroType = AvroFloat

  protected[scalavro] def asGeneric[F <: Float : TypeTag](value: F): Float = value

  protected[scalavro] def fromGeneric(obj: Any): Float = obj.asInstanceOf[Float]

  def write[F <: Float : TypeTag](value: F, stream: OutputStream) = {
    val encoder = EncoderFactory.get.directBinaryEncoder(stream, null)
    encoder writeFloat value
  }

  def read(stream: InputStream) = Try {
    val decoder = DecoderFactory.get.directBinaryDecoder(stream, null)
    decoder.readFloat
  }
}
