package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.error.AvroDeserializationException
import com.gensler.scalavro.types.primitive.AvroNull
import org.apache.avro.Schema
import org.apache.avro.io.{ BinaryDecoder, BinaryEncoder }
import spray.json._

import scala.util.Try

object AvroNullIO extends AvroNullIO

trait AvroNullIO extends AvroPrimitiveTypeIO[Unit] {

  val avroType = AvroNull

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  // null is written as zero bytes.
  protected[scalavro] def write(
    value: Unit,
    encoder: BinaryEncoder): Unit = {}

  override protected[scalavro] def read(decoder: BinaryDecoder, writerSchema: Option[Schema]) = ()

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  def writePrimitiveJson(value: Unit): JsValue = JsNull

  def readJson(json: JsValue) = Try {
    json match {
      case JsNull => Unit
      case _      => throw new AvroDeserializationException[Unit]
    }
  }
}