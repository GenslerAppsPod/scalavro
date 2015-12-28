package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.types.primitive.AvroString
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import org.apache.avro.Schema

import org.apache.avro.generic.GenericData
import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }
import scala.collection.JavaConversions._

import spray.json._

import scala.util.Try

object AvroStringIO extends AvroStringIO

trait AvroStringIO extends AvroNullablePrimitiveTypeIO[String] {

  val avroType = AvroString

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  protected[scalavro] def write(
    value: String,
    encoder: BinaryEncoder): Unit =
    if (value == null) {
      AvroLongIO.write(UNION_INDEX_NULL, encoder)
    }
    else {
      AvroLongIO.write(UNION_INDEX_VALUE, encoder)
      encoder writeString value
    }
  private[scalavro] def readNotNull(decoder: BinaryDecoder, writerSchema: Option[Schema]) = decoder.readString

  private def readNullable(decoder: BinaryDecoder, readString: () => String): String =
    AvroLongIO.read(decoder) match {
      case UNION_INDEX_NULL  => null
      case UNION_INDEX_VALUE => readString()
    }

  private def readStringFromBytes(decoder: BinaryDecoder): String = new String(AvroBytesIO.read(decoder, None).toArray, "UTF-8")

  private def types(schema: Schema): Set[Schema.Type] = schema.getTypes.toSet.map { s: Schema => s.getType }
  private def nullableString(schema: Schema) = types(schema) == Set(Schema.Type.STRING, Schema.Type.NULL)
  private def nullableBytes(schema: Schema) = types(schema) == Set(Schema.Type.BYTES, Schema.Type.NULL)

  override protected[scalavro] def read(decoder: BinaryDecoder, writerSchema: Option[Schema]): String =
    writerSchema.map({ schema =>
      schema.getType match {
        case Schema.Type.STRING                          => decoder.readString
        case Schema.Type.BYTES                           => readStringFromBytes(decoder)
        case Schema.Type.UNION if nullableString(schema) => readNullable(decoder, () => decoder.readString)
        case Schema.Type.UNION if nullableBytes(schema)  => readNullable(decoder, () => readStringFromBytes(decoder))
        case writerType                                  => throw new IllegalArgumentException(s"cannot transform from ${writerType} to String")
      }
    }).getOrElse(readNullable(decoder, () => decoder.readString))

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  def writePrimitiveJson(value: String) =
    if (value == null)
      JsNull
    else
      JsString(value)

  def readJson(json: JsValue): Try[String] = Try {
    json match {
      case JsString(value) => value
      case JsNull          => null
      case _               => throw new AvroDeserializationException[String]
    }
  }

}
