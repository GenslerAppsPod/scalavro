package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroDouble
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

object AvroDoubleIO extends AvroDoubleIO

trait AvroDoubleIO extends AvroTypeIO[Double] {

  def avroType = AvroDouble

  protected[scalavro] def asGeneric[D <: Double: TypeTag](value: D): Double = value

  def write[D <: Double: TypeTag](value: D, encoder: BinaryEncoder) = {
    encoder writeDouble value
    encoder.flush
  }

  def read(decoder: BinaryDecoder) = Try {
    decoder.readDouble
  }
}