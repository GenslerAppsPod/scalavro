package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.types.primitive.AvroBytes
import org.apache.avro.Schema
import org.apache.avro.io.{ BinaryDecoder, BinaryEncoder }
import spray.json._

object AvroBytesIO extends AvroBytesIO

trait AvroBytesIO extends AvroPrimitiveTypeIO[Seq[Byte]] {

  val avroType = AvroBytes

  ////////////////////////////////////////////////////////////////////////////
  // BINARY ENCODING
  ////////////////////////////////////////////////////////////////////////////

  protected[scalavro] def write(
    bytes: Seq[Byte],
    encoder: BinaryEncoder): Unit = AvroByteArrayIO.write(bytes.toArray, encoder)

  override protected[scalavro] def read(decoder: BinaryDecoder, writerSchema: Option[Schema]) = AvroByteArrayIO.read(decoder, writerSchema).toIndexedSeq

  ////////////////////////////////////////////////////////////////////////////
  // JSON ENCODING
  ////////////////////////////////////////////////////////////////////////////

  def writePrimitiveJson(bytes: Seq[Byte]) = AvroByteArrayIO.writePrimitiveJson(bytes.toArray)

  def readJson(json: JsValue) = AvroByteArrayIO.readJson(json).map(_.toSeq)

}