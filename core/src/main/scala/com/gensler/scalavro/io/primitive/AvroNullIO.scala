package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroNull
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

object AvroNullIO extends AvroNullIO

trait AvroNullIO extends AvroTypeIO[Unit] {

  def avroType = AvroNull

  protected[scalavro] def asGeneric[U <: Unit: TypeTag](value: U): Unit = Unit

  // null is written as zero bytes.
  def write[U <: Unit: TypeTag](value: U, encoder: BinaryEncoder) {}

  def read(decoder: BinaryDecoder) = Success(())

}