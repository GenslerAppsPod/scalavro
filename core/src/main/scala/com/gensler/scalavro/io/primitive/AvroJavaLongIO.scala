package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.types.primitive.AvroJavaLong
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import org.apache.avro.Schema

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import spray.json._

import scala.util.Try

object AvroJavaLongIO extends AvroJavaLongIO

trait AvroJavaLongIO extends AvroNullablePrimitiveTypeIO[java.lang.Long] {

  val avroType = AvroJavaLong

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  protected[scalavro] def write(
    value: java.lang.Long,
    encoder: BinaryEncoder): Unit =
    if (value == null) {
      AvroLongIO.write(UNION_INDEX_NULL, encoder)
    }
    else {
      AvroLongIO.write(UNION_INDEX_VALUE, encoder)
      encoder writeLong value
    }

  override private[scalavro] def readNotNull(decoder: BinaryDecoder, writerSchema: Option[Schema]) = AvroLongIO.read(decoder, writerSchema)

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  def writePrimitiveJson(value: java.lang.Long) =
    if (value == null)
      JsNull
    else
      JsNumber(BigDecimal(value))

  def readJson(json: JsValue): Try[java.lang.Long] = Try {
    json match {
      case JsNumber(bigDecimal) if bigDecimal.isValidLong => bigDecimal.toLong
      case JsNull => null
      case _ => throw new AvroDeserializationException[java.lang.Long]
    }
  }

}