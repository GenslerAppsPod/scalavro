package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.types.primitive.AvroLong
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import org.apache.avro.Schema

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import spray.json._

import scala.util.Try

object AvroLongIO extends AvroLongIO

trait AvroLongIO extends AvroPrimitiveTypeIO[Long] {

  val avroType = AvroLong

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  protected[scalavro] def write(
    value: Long,
    encoder: BinaryEncoder): Unit = encoder writeLong value

  override protected[scalavro] def read(decoder: BinaryDecoder, writerSchema: Option[Schema]): Long = decoder.readLong
  protected[scalavro] def read(decoder: BinaryDecoder): Long = read(decoder, None)

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  def writePrimitiveJson(value: Long) = JsNumber(BigDecimal(value))

  def readJson(json: JsValue) = Try {
    json match {
      case JsNumber(bigDecimal) if bigDecimal.isValidLong => bigDecimal.toLong
      case _ => throw new AvroDeserializationException[Long]
    }
  }

}