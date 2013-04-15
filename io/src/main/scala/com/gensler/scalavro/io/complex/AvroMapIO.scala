package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.complex.AvroMap
import com.gensler.scalavro.error.{AvroSerializationException, AvroDeserializationException}

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericArray, GenericDatumWriter, GenericDatumReader}
import org.apache.avro.io.{EncoderFactory, DecoderFactory}
import org.apache.avro.util.Utf8

import scala.util.{Try, Success, Failure}
import scala.reflect.runtime.universe.TypeTag

import java.io.{InputStream, OutputStream}

case class AvroMapIO[T](avroType: AvroMap[T]) extends AvroTypeIO[Map[String, T]] {

  implicit def itemTypeTag = avroType.itemType.tag

  protected lazy val avroSchema: Schema = (new Parser) parse avroType.selfContainedSchema().toString
  val itemIO = AvroTypeIO.Implicits.avroTypeToIO(avroType.itemType)

  protected[scalavro] def asGeneric[M <: Map[String, T] : TypeTag](map: M): java.util.Map[String, T] =
    java.util.Collections.unmodifiableMap(
      scala.collection.JavaConversions mapAsJavaMap map
    )

  protected[scalavro] def fromGeneric(obj: Any): Map[String, T] = {
    import scala.collection.JavaConversions.mapAsScalaMap
    import com.gensler.scalavro.io.AvroTypeIO.Implicits._

    obj match {
      case map: java.util.Map[_, _] => {
        val genericMap = mapAsScalaMap(map).asInstanceOf[scala.collection.Map[Utf8, _]]
        genericMap.map {
          case (key, value) => key.toString -> avroType.itemType.fromGeneric(value)
        }.toIndexedSeq.toMap
      }
      case _ => throw new AvroDeserializationException()(avroType.tag)
    }
  }

  def write[M <: Map[String, T] : TypeTag](map: M, stream: OutputStream) = {
    try {
      val datumWriter = new GenericDatumWriter[java.util.Map[String, T]](avroSchema)
      val encoder = EncoderFactory.get.binaryEncoder(stream, null)
      datumWriter.write(asGeneric(map), encoder)
      encoder.flush
    }
    catch { case cause: Throwable => 
      throw new AvroSerializationException(map, cause)
    }
  }

  def read(stream: InputStream) = Try {
    val datumReader = new GenericDatumReader[java.util.Map[String, T]](avroSchema)
    val decoder = DecoderFactory.get.directBinaryDecoder(stream, null)
    this fromGeneric datumReader.read(null, decoder)
  }

}