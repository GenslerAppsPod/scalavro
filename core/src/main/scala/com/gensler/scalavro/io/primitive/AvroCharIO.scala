package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.types.primitive.AvroChar
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import org.apache.avro.Schema

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import spray.json._

import scala.util.Try

object AvroCharIO extends AvroCharIO

trait AvroCharIO extends AvroPrimitiveTypeIO[Char] {

  val avroType = AvroChar

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  protected[scalavro] def write(
    value: Char,
    encoder: BinaryEncoder): Unit = encoder writeInt value.toChar

  override protected[scalavro] def read(decoder: BinaryDecoder, writerSchema: Option[Schema]) = decoder.readInt.toChar

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  def writePrimitiveJson(value: Char) = JsString(value.toString)

  def readJson(json: JsValue) = Try {
    json match {
      case JsString(value) if value.length == 1 => value.head
      case _                                    => throw new AvroDeserializationException[Char]
    }
  }

}