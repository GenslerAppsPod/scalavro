package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroLong
import com.gensler.scalavro.error.{AvroSerializationException, AvroDeserializationException}

import org.apache.avro.io.{EncoderFactory, DecoderFactory}

import scala.util.{Try, Success, Failure}
import scala.reflect.runtime.universe.TypeTag

import java.io.{InputStream, OutputStream}

object AvroLongIO extends AvroLongIO

trait AvroLongIO extends AvroTypeIO[Long] {

  def avroType = AvroLong

  protected[scalavro] def asGeneric[L <: Long : TypeTag](value: L): Long = value

  protected[scalavro] def fromGeneric(obj: Any): Long = obj.asInstanceOf[Long]

  def write[L <: Long : TypeTag](value: L, stream: OutputStream) = {
    val encoder = EncoderFactory.get.directBinaryEncoder(stream, null)
    encoder writeLong value
  }

  def read(stream: InputStream) = Try {
    val decoder = DecoderFactory.get.directBinaryDecoder(stream, null)
    decoder.readLong
  }

}