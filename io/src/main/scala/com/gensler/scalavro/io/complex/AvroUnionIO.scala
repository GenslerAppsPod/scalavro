package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.io.primitive.AvroLongIO
import com.gensler.scalavro.types.AvroType
import com.gensler.scalavro.types.complex.AvroUnion
import com.gensler.scalavro.error.{AvroSerializationException, AvroDeserializationException}
import com.gensler.scalavro.io.AvroTypeIO.Implicits._

import com.gensler.scalavro.util.Union

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericArray, GenericDatumWriter, GenericDatumReader}
import org.apache.avro.io.{EncoderFactory, DecoderFactory}

import scala.util.{Try, Success, Failure}
import scala.reflect.runtime.universe.{TypeTag, typeOf}

import java.io.{InputStream, OutputStream}

case class AvroUnionIO[U <: Union.not[_]](avroType: AvroUnion[U]) extends AvroTypeIO[U] {

  protected lazy val avroSchema: Schema = (new Parser) parse avroType.selfContainedSchema().toString

  protected[scalavro] def asGeneric[T <: U : TypeTag](obj: T) = {
    avroType.memberAvroTypes.find { at => typeOf[T] <:< at.tag.tpe } match {
      case Some(memberType) => {
        memberType.asInstanceOf[AvroType[T]].asGeneric(obj)
      }
      case None => throw new AvroSerializationException(obj)
    }
  }

  protected[scalavro] def fromGeneric(obj: Any): U = {

    println("AvroUnionIO.fromGeneric -- received an object of type" format obj.getClass.getName)

    // get union index, look up proper type, call its fromGeneric
    // Not sure yet how the Apache implementation reads generic unions...

    ???
  }

  def write[T <: U : TypeTag](obj: T, stream: OutputStream) = {
    avroType.memberAvroTypes.indexWhere { at => typeOf[T] <:< at.tag.tpe } match {
      case -1    => throw new AvroSerializationException(obj)
      case index => {
        try {
          val datumWriter = new GenericDatumWriter[Any](avroSchema)
          val encoder = EncoderFactory.get.binaryEncoder(stream, null)

          AvroLongIO.write(index.toLong, stream)
          datumWriter.write(asGeneric(obj), encoder)
          encoder.flush
        }
        catch { case cause: Throwable =>
          throw new AvroSerializationException(obj, cause)
        }
      }
    }
  }

  def read(stream: InputStream) = Try {
    val index = AvroLongIO.read(stream).get
    avroType.memberAvroTypes(index.toInt).read(stream).get.asInstanceOf[U]
  }

}