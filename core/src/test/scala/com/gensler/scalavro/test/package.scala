package com.gensler.scalavro

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }

import com.gensler.scalavro.types.AvroType
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericDatumReader, GenericData, GenericDatumWriter, GenericRecord }
import org.apache.avro.io.{ DecoderFactory, EncoderFactory }

import scala.reflect.runtime.universe._
import scala.util.Success

package object test {

  def write(schema: Schema)(fill: (GenericData.Record) => Unit): Array[Byte] = {
    val p = new GenericData.Record(schema)
    fill(p)

    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream

    val encoder = EncoderFactory.get.directBinaryEncoder(out, null)
    datumWriter.write(p, encoder)
    out.toByteArray
  }

  def write[T: TypeTag](record: T): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    AvroType[T].io.write(record, out)
    out.toByteArray
  }

  def read[T: TypeTag](data: Array[Byte]): T = {
    val Success(result) = AvroType[T].io.read(new ByteArrayInputStream(data))
    result
  }

  def read(data: Array[Byte], schema: Schema): GenericData.Record = {
    val datumReader = new GenericDatumReader[GenericData.Record](schema)
    val decoder = DecoderFactory.get.directBinaryDecoder(new ByteArrayInputStream(data), null)
    datumReader.read(null, decoder)
  }

  def toHex(data: Array[Byte]) = {
    data.map(b => String.format("%02X ", java.lang.Byte.valueOf(b))).mkString
  }

}
