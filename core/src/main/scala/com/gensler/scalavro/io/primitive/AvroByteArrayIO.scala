package com.gensler.scalavro.io.primitive

import java.nio.CharBuffer
import java.nio.charset.Charset

import com.gensler.scalavro.error.AvroDeserializationException
import com.gensler.scalavro.types.primitive.AvroByteArray
import org.apache.avro.Schema
import org.apache.avro.io.{ BinaryDecoder, BinaryEncoder }
import spray.json._

import scala.util.Try

object AvroByteArrayIO extends AvroByteArrayIO

trait AvroByteArrayIO extends AvroPrimitiveTypeIO[Array[Byte]] {

  val avroType = AvroByteArray

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  protected[scalavro] def write(
    bytes: Array[Byte],
    encoder: BinaryEncoder): Unit = encoder writeBytes bytes

  private def readBytes(decoder: BinaryDecoder): Array[Byte] = {
    val numBytes = decoder.readLong
    val buffer = Array.ofDim[Byte](numBytes.toInt)
    decoder.readFixed(buffer)
    buffer
  }

  override protected[scalavro] def read(decoder: BinaryDecoder, writerSchema: Option[Schema]): Array[Byte] =
    writerSchema.map({ schema =>
      schema.getType match {
        case Schema.Type.STRING | Schema.Type.BYTES => readBytes(decoder)
        case writerType                             => throw new IllegalArgumentException(s"cannot transform from ${writerType} to bytes")
      }
    }).getOrElse(readBytes(decoder));

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  val utf8: Charset = Charset.forName("UTF-8")
  val utf8Encoder = utf8.newEncoder

  def writePrimitiveJson(bytes: Array[Byte]) = {
    val utf8String = new String(bytes, utf8)
    JsString(utf8String)
  }

  def readJson(json: JsValue) = Try {
    json match {
      case JsString(value) => {
        val byteBuf = utf8Encoder.encode(CharBuffer.wrap(value))
        val bytes = new Array[Byte](byteBuf.remaining)
        byteBuf.get(bytes)
        bytes
      }
      case _ => throw new AvroDeserializationException[Array[Byte]]
    }
  }

}