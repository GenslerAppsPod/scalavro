package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroBytes
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

import java.nio.ByteBuffer

object AvroBytesIO extends AvroBytesIO

trait AvroBytesIO extends AvroTypeIO[Seq[Byte]] {

  def avroType = AvroBytes

  protected[scalavro] def asGeneric[B <: Seq[Byte]: TypeTag](value: B): ByteBuffer = ByteBuffer.wrap(value.toArray)

  def write[B <: Seq[Byte]: TypeTag](bytes: B, encoder: BinaryEncoder) = {
    encoder writeBytes bytes.toArray
    encoder.flush
  }

  def read(decoder: BinaryDecoder) = Try {
    val numBytes = decoder.readLong
    val buffer = Array.ofDim[Byte](numBytes.toInt)
    decoder.readFixed(buffer)
    buffer.toIndexedSeq
  }

}