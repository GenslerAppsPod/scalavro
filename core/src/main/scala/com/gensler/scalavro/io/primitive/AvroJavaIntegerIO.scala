package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.error.AvroDeserializationException
import com.gensler.scalavro.types.primitive.AvroJavaInteger
import org.apache.avro.Schema
import org.apache.avro.io.{ BinaryDecoder, BinaryEncoder }
import spray.json._

import scala.util.Try

object AvroJavaIntegerIO extends AvroJavaIntegerIO

trait AvroJavaIntegerIO extends AvroNullablePrimitiveTypeIO[java.lang.Integer] {

  val avroType = AvroJavaInteger

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  protected[scalavro] def write(
    value: java.lang.Integer,
    encoder: BinaryEncoder): Unit =
    if (value == null) {
      AvroLongIO.write(UNION_INDEX_NULL, encoder)
    }
    else {
      AvroLongIO.write(UNION_INDEX_VALUE, encoder)
      encoder writeInt value
    }

  override private[scalavro] def readNotNull(decoder: BinaryDecoder, writerSchema: Option[Schema]) = AvroIntIO.read(decoder, writerSchema)

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  def writePrimitiveJson(value: java.lang.Integer) =
    if (value == null)
      JsNull
    else
      JsNumber(BigDecimal(value))

  def readJson(json: JsValue): Try[java.lang.Integer] = Try {
    json match {
      case JsNumber(bigDecimal) if bigDecimal.isValidInt => bigDecimal.toInt
      case JsNull => null
      case _ => throw new AvroDeserializationException[java.lang.Integer]
    }
  }

}