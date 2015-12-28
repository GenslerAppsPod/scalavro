package com.gensler.scalavro.io.primitive

import org.apache.avro.Schema
import org.apache.avro.io.BinaryDecoder

trait AvroNullablePrimitiveTypeIO[T <: AnyRef] extends AvroPrimitiveTypeIO[T] {

  protected val UNION_INDEX_NULL: Long = 0
  protected val UNION_INDEX_VALUE: Long = 1

  private[scalavro] def readNotNull(decoder: BinaryDecoder, writerSchema: Option[Schema]): T

  override protected[scalavro] def read(decoder: BinaryDecoder, writerSchema: Option[Schema]): T =
    AvroLongIO.read(decoder) match {
      case UNION_INDEX_NULL  => null.asInstanceOf[T]
      case UNION_INDEX_VALUE => readNotNull(decoder, writerSchema)
    }

}
