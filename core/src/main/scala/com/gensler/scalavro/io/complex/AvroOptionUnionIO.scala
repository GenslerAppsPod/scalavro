package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.io.primitive.{ AvroLongIO, AvroNullIO }
import com.gensler.scalavro.types.AvroType
import com.gensler.scalavro.types.complex.AvroUnion
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import com.gensler.scalavro.util.Union
import com.gensler.scalavro.util.Union._

import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

import java.io.{ InputStream, OutputStream }

private[scalavro] case class AvroOptionUnionIO[U <: Union.not[_]: TypeTag, T <: Option[_]: TypeTag](
    avroType: AvroUnion[U, T]) extends AvroUnionIO[U, T] {

  // IMPORTANT:
  // null is the 0th index in the union, per AvroType.fromType
  val (nullIndex, nonNullIndex) = (0L, 1L)

  val TypeRef(_, _, List(innerType)) = typeOf[T]

  val innerAvroType = avroType.memberAvroTypes.find { at => innerType <:< at.tag.tpe }.get

  def write[X <: T: TypeTag](obj: X, encoder: BinaryEncoder) = {
    AvroLongIO.write(if (obj.isDefined) nonNullIndex else nullIndex, encoder)
    writeHelper(obj, encoder)(typeTag[X], innerAvroType.tag)
    encoder.flush
  }

  def writeHelper[X <: T: TypeTag, A: TypeTag](obj: X, encoder: BinaryEncoder) =
    obj match {
      case Some(value) => innerAvroType.asInstanceOf[AvroType[A]].io.write(value.asInstanceOf[A], encoder)
      case None        => AvroNullIO.write((), encoder)
    }

  def read(decoder: BinaryDecoder) = Try {
    readHelper(decoder)(innerAvroType.tag).asInstanceOf[T]
  }

  def readHelper[A: TypeTag](decoder: BinaryDecoder) = {
    val index = AvroLongIO.read(decoder).get
    if (index == nonNullIndex) Some(innerAvroType.io.read(decoder).get.asInstanceOf[A])
    else if (index == nullIndex) None
    else throw new AvroDeserializationException[T](
      detailedMessage = "Encountered an index that was not zero or one: [%s]" format index
    )
  }
}