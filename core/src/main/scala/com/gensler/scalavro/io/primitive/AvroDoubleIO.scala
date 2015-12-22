package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.types.primitive.AvroDouble
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import org.apache.avro.Schema

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import spray.json._

import scala.util.Try

object AvroDoubleIO extends AvroDoubleIO

trait AvroDoubleIO extends AvroPrimitiveTypeIO[Double] {

  val avroType = AvroDouble

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  protected[scalavro] def write(
    value: Double,
    encoder: BinaryEncoder): Unit = encoder writeDouble value

  override protected[scalavro] def read(decoder: BinaryDecoder, writerSchema: Option[Schema]): Double =
    writerSchema.map { schema =>
      schema.getType match {
        case Schema.Type.DOUBLE => decoder.readDouble
        case Schema.Type.FLOAT  => decoder.readFloat.toDouble
        case Schema.Type.LONG   => decoder.readLong.toDouble
        case Schema.Type.INT    => decoder.readInt.toDouble
        case _                  => throw new AvroDeserializationException[Float](detailedMessage = s"cannot convert from ${schema.getType} to double")
      }
    }.getOrElse(decoder.readDouble)

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  def writePrimitiveJson(value: Double) = JsNumber(BigDecimal(value))

  def readJson(json: JsValue) = Try {
    json match {
      case JsNumber(bigDecimal) => bigDecimal.toDouble
      case _                    => throw new AvroDeserializationException[Double]
    }
  }

}