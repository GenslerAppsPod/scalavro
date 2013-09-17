package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.complex.AvroUnion
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import com.gensler.scalavro.util.Union
import com.gensler.scalavro.util.Union._

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

object AvroUnionIO {
  def apply[U <: Union.not[_]: TypeTag, T: TypeTag](avroType: AvroUnion[U, T]): AvroUnionIO[U, T] = {

    val isEitherUnion = avroType.originalType.tpe <:< typeOf[Either[_, _]]
    val isOptionUnion = avroType.originalType.tpe <:< typeOf[Option[_]]
    val isBareUnion = avroType.originalType.tpe <:< typeOf[Union[_]] || avroType.originalType.tpe <:< typeOf[Union.not[_]]
    val isClassUnion = !isEitherUnion && !isOptionUnion && !isBareUnion

    if (isEitherUnion) {
      val castedUnion: AvroUnion[U, Either[_, _]] = avroType.asInstanceOf[AvroUnion[U, Either[_, _]]]
      val eitherUnionIO = AvroEitherUnionIO(castedUnion)(typeTag[U], typeTag[T].asInstanceOf[TypeTag[Either[_, _]]])
      eitherUnionIO.asInstanceOf[AvroUnionIO[U, T]]
    }
    else if (isOptionUnion) {
      val castedUnion: AvroUnion[U, Option[_]] = avroType.asInstanceOf[AvroUnion[U, Option[_]]]
      val optionUnionIO = AvroOptionUnionIO(castedUnion)(typeTag[U], typeTag[T].asInstanceOf[TypeTag[Option[_]]])
      optionUnionIO.asInstanceOf[AvroUnionIO[U, T]]
    }
    else if (isBareUnion) AvroBareUnionIO(avroType)(typeTag[U], typeTag[T])

    else if (isClassUnion) AvroClassUnionIO(avroType)(typeTag[U], typeTag[T])

    else throw new IllegalArgumentException("The provided AvroUnion is unrecognized!")
  }
}

abstract class AvroUnionIO[U <: Union.not[_]: TypeTag, T: TypeTag] extends AvroTypeIO[T] {
  protected lazy val avroSchema: Schema = (new Parser) parse avroType.selfContainedSchema().toString
}
