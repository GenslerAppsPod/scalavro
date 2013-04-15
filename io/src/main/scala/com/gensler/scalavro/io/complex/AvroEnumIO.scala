package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.complex.AvroEnum
import com.gensler.scalavro.error.{AvroSerializationException, AvroDeserializationException}
import com.gensler.scalavro.util.ReflectionHelpers

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericEnumSymbol, GenericDatumWriter, GenericDatumReader}
import org.apache.avro.io.{EncoderFactory, DecoderFactory}

import scala.util.{Try, Success, Failure}
import scala.reflect.runtime.universe.TypeTag

import java.io.{InputStream, OutputStream}

case class AvroEnumIO[E <: Enumeration](avroType: AvroEnum[E]) extends AvroTypeIO[E#Value] {

  // AvroEnum exposes two TypeTags:
  //   `AvroEnum.tag` is the TypeTag for the enum values
  //   `AvroEnum.enumTag` is the TypeTag of the enum itself

  protected lazy val avroSchema: Schema = (new Parser) parse avroType.selfContainedSchema().toString

  val moduleMirror = ReflectionHelpers.classLoaderMirror.reflectModule {
    avroType.enumTag.tpe.typeSymbol.asClass.module.asModule
  }

  val enumeration = moduleMirror.instance.asInstanceOf[E]

  protected[scalavro] def asGeneric[T <: E#Value : TypeTag](obj: T): GenericEnumSymbol = obj match {
    case value: E#Value => new GenericData.EnumSymbol(avroSchema, value.toString)
    case _ => throw new AvroSerializationException(obj)
  }

  protected[scalavro] def fromGeneric(obj: Any): E#Value = obj match {
    case genericEnumSymbol: GenericEnumSymbol => enumeration withName genericEnumSymbol.toString
    case _ => throw new AvroDeserializationException[E#Value]()(avroType.tag)
  }

  def write[T <: E#Value : TypeTag](obj: T, stream: OutputStream) = {
    try {
      val datumWriter = new GenericDatumWriter[GenericEnumSymbol](avroSchema)
      val encoder = EncoderFactory.get.binaryEncoder(stream, null)
      datumWriter.write(asGeneric(obj), encoder)
      encoder.flush
    }
    catch { case cause: Throwable => 
      throw new AvroSerializationException(obj, cause)
    }
  }

  def read(stream: InputStream) = Try {
    val datumReader = new GenericDatumReader[GenericEnumSymbol](avroSchema)
    val decoder = DecoderFactory.get.directBinaryDecoder(stream, null)
    this fromGeneric datumReader.read(null, decoder)
  }

}