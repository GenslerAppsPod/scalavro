package com.gensler.scalavro

import java.io.ByteArrayOutputStream

import com.gensler.scalavro.types.AvroType
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, GenericDatumWriter, GenericRecord }
import org.apache.avro.io.EncoderFactory

import scala.reflect.runtime.universe._

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

  def toHex(data: Array[Byte]) = {
    data.map(b => String.format("%02X ", java.lang.Byte.valueOf(b))).mkString
  }

}
