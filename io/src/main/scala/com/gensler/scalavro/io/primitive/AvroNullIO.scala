package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroNull
import com.gensler.scalavro.error.{AvroSerializationException, AvroDeserializationException}

import org.apache.avro.generic.GenericData

import scala.util.{Try, Success, Failure}
import scala.reflect.runtime.universe.TypeTag

import java.io.{InputStream, OutputStream}

object AvroNullIO extends AvroNullIO

trait AvroNullIO extends AvroTypeIO[Unit] {

  def avroType = AvroNull

  protected[scalavro] def asGeneric[U <: Unit : TypeTag](value: U): Unit = Unit

  protected[scalavro] def fromGeneric(obj: Any): Unit = obj.asInstanceOf[Unit]

  // null is written as zero bytes.
  def write[U <: Unit : TypeTag](value: U, stream: OutputStream) {}

  def read(stream: InputStream) = Success(())

}