package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.types.primitive.AvroBytes
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import org.apache.avro.Schema

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import spray.json._

import scala.util.Try

import java.nio.{ ByteBuffer, CharBuffer }
import java.nio.charset.Charset

object AvroBytesIO extends AvroBytesIO

trait AvroBytesIO extends AvroPrimitiveTypeIO[Seq[Byte]] {

  val avroType = AvroBytes

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  protected[scalavro] def write(
    bytes: Seq[Byte],
    encoder: BinaryEncoder): Unit = encoder writeBytes bytes.toArray

  override protected[scalavro] def read(decoder: BinaryDecoder, writerSchema: Option[Schema]) = {
    val numBytes = decoder.readLong
    val buffer = Array.ofDim[Byte](numBytes.toInt)
    decoder.readFixed(buffer)
    buffer.toIndexedSeq
  }

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  val utf8: Charset = Charset.forName("UTF-8")
  val utf8Encoder = utf8.newEncoder

  def writePrimitiveJson(bytes: Seq[Byte]) = {
    val utf8String = new String(bytes.toArray, utf8)
    JsString(utf8String)
  }

  def readJson(json: JsValue) = Try {
    json match {
      case JsString(value) => {
        val byteBuf = utf8Encoder.encode(CharBuffer.wrap(value))
        val bytes = new Array[Byte](byteBuf.remaining)
        byteBuf.get(bytes)
        bytes.toSeq
      }
      case _ => throw new AvroDeserializationException[Seq[Byte]]
    }
  }

}