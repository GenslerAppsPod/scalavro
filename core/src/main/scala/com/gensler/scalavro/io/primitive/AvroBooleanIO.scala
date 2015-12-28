package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.types.primitive.AvroBoolean
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import org.apache.avro.Schema

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import spray.json._

import scala.util.Try

object AvroBooleanIO extends AvroBooleanIO

trait AvroBooleanIO extends AvroPrimitiveTypeIO[Boolean] {

  val avroType = AvroBoolean

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  /**
    * a boolean is written as a single byte whose value is either 0 (false) or
    * 1 (true).
    */
  protected[scalavro] def write(
    value: Boolean,
    encoder: BinaryEncoder): Unit = encoder writeBoolean value

  override protected[scalavro] def read(decoder: BinaryDecoder, writerSchema: Option[Schema]) = decoder.readBoolean

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  def writePrimitiveJson(value: Boolean) = JsBoolean(value)

  def readJson(json: JsValue) = Try {
    json match {
      case JsBoolean(value) => value
      case _                => throw new AvroDeserializationException[Boolean]
    }
  }

}