package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.complex.AvroArray
import com.gensler.scalavro.error.{AvroSerializationException, AvroDeserializationException}

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericArray, GenericDatumWriter, GenericDatumReader}
import org.apache.avro.io.{EncoderFactory, DecoderFactory}

import scala.util.{Try, Success, Failure}
import scala.reflect.runtime.universe.TypeTag

import java.io.{InputStream, OutputStream}

case class AvroArrayIO[T](avroType: AvroArray[T]) extends AvroTypeIO[Seq[T]] {

  implicit def itemTypeTag = avroType.itemType.tag

  protected lazy val avroSchema: Schema = (new Parser) parse avroType.selfContainedSchema().toString
  val itemIO = AvroTypeIO.Implicits.avroTypeToIO(avroType.itemType)

  protected[scalavro] def asGeneric[G <: Seq[T] : TypeTag](items: G): GenericArray[Any] = {
    import scala.collection.JavaConversions.seqAsJavaList
    val genericArray = new GenericData.Array[Any](items.size, avroSchema)
    genericArray addAll seqAsJavaList(items map { itemIO.asGeneric })
    genericArray
  }

  protected[scalavro] def fromGeneric(obj: Any): Seq[T] = {
    import scala.collection.JavaConversions.asScalaBuffer
    obj match {
      case genericArray: GenericArray[_] => {
        val genericSeq = asScalaBuffer(genericArray)
        genericSeq.map { itemIO.fromGeneric }
      }
      case _ => throw new AvroDeserializationException()(avroType.tag)
    }
  }

  def write[G <: Seq[T] : TypeTag](obj: G, stream: OutputStream) = {
    try {
      val datumWriter = new GenericDatumWriter[GenericArray[_]](avroSchema)
      val encoder = EncoderFactory.get.binaryEncoder(stream, null)
      datumWriter.write(asGeneric(obj), encoder)
      encoder.flush
    }
    catch { case cause: Throwable => 
      throw new AvroSerializationException(obj, cause)
    }
  }

  def read(stream: InputStream) = Try {
    val datumReader = new GenericDatumReader[GenericArray[_]](avroSchema)
    val decoder = DecoderFactory.get.directBinaryDecoder(stream, null)
    this fromGeneric datumReader.read(null, decoder)
  }

}