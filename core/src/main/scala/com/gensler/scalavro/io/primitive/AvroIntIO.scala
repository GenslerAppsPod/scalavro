package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.types.primitive.AvroInt
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import org.apache.avro.Schema

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import spray.json._

import scala.util.Try

object AvroIntIO extends AvroIntIO

trait AvroIntIO extends AvroPrimitiveTypeIO[Int] {

  val avroType = AvroInt

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  protected[scalavro] def write(
    value: Int,
    encoder: BinaryEncoder): Unit = encoder writeInt value

  override protected[scalavro] def read(decoder: BinaryDecoder, writerSchema: Option[Schema]) = decoder.readInt

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  def writePrimitiveJson(value: Int) = JsNumber(BigDecimal(value))

  def readJson(json: JsValue) = Try {
    json match {
      case JsNumber(bigDecimal) if bigDecimal.isValidInt => bigDecimal.toInt
      case _ => throw new AvroDeserializationException[Int]
    }
  }

}