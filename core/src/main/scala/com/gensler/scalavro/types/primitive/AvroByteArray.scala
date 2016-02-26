package com.gensler.scalavro.types.primitive

import com.gensler.scalavro.types.AvroPrimitiveType

/**
  * Represents a mapping from Array[Byte] to the corresponding Avro type.
  */
trait AvroByteArray extends AvroPrimitiveType[Array[Byte]] {
  override val typeName = "bytes"
}
object AvroByteArray extends AvroByteArray
