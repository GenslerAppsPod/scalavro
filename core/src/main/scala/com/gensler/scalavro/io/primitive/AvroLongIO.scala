package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroLong
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

object AvroLongIO extends AvroLongIO

trait AvroLongIO extends AvroTypeIO[Long] {

  def avroType = AvroLong

  protected[scalavro] def asGeneric[L <: Long: TypeTag](value: L): Long = value

  def write[L <: Long: TypeTag](value: L, encoder: BinaryEncoder) = {
    encoder writeLong value
    encoder.flush
  }

  def read(decoder: BinaryDecoder) = Try {
    decoder.readLong
  }

}