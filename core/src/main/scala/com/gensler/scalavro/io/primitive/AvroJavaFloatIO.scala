package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.error.AvroDeserializationException
import com.gensler.scalavro.types.primitive.AvroJavaFloat
import org.apache.avro.Schema
import org.apache.avro.io.{ BinaryDecoder, BinaryEncoder }
import spray.json._

import scala.util.Try

object AvroJavaFloatIO extends AvroJavaFloatIO

trait AvroJavaFloatIO extends AvroNullablePrimitiveTypeIO[java.lang.Float] {

  val avroType = AvroJavaFloat

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  protected[scalavro] def write(
    value: java.lang.Float,
    encoder: BinaryEncoder): Unit =
    if (value == null) {
      AvroLongIO.write(UNION_INDEX_NULL, encoder)
    }
    else {
      AvroLongIO.write(UNION_INDEX_VALUE, encoder)
      encoder writeFloat value
    }

  override private[scalavro] def readNotNull(decoder: BinaryDecoder, writerSchema: Option[Schema]) = AvroFloatIO.read(decoder, writerSchema)

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  def writePrimitiveJson(value: java.lang.Float) =
    if (value == null)
      JsNull
    else
      JsNumber(value.toDouble)

  def readJson(json: JsValue): Try[java.lang.Float] = Try {
    json match {
      case JsNumber(bigDecimal) => bigDecimal.toFloat
      case JsNull               => null
      case _                    => throw new AvroDeserializationException[java.lang.Float]
    }
  }

}
