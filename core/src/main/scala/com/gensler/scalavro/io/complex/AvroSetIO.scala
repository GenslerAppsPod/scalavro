package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.io.primitive.AvroLongIO
import com.gensler.scalavro.types.complex.AvroSet
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import com.gensler.scalavro.util.ReflectionHelpers

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{ GenericData, GenericArray, GenericDatumWriter }
import org.apache.avro.io.{ BinaryEncoder, BinaryDecoder }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

import java.io.{ InputStream, OutputStream }

case class AvroSetIO[T, S <: Set[T]](avroType: AvroSet[T, S]) extends AvroTypeIO[S]()(avroType.originalTypeTag) {

  implicit def itemTypeTag = avroType.itemType.tag
  implicit def originalTypeTag = avroType.originalTypeTag

  val originalTypeVarargsApply = ReflectionHelpers.companionVarargsApply[S] match {
    case Some(methodMirror) => methodMirror
    case None => throw new IllegalArgumentException(
      "Sequence subclasses must have a companion object with a public varargs " +
        "apply method, but no such method was found for type [%s].".format(avroType.originalTypeTag.tpe)
    )
  }

  def write[G <: Set[T]: TypeTag](items: G, encoder: BinaryEncoder) = {
    try {
      encoder.writeArrayStart
      encoder.setItemCount(items.size)
      for (item <- items) {
        encoder.startItem
        avroType.itemType.io.write(item, encoder)
      }
      encoder.writeArrayEnd
      encoder.flush
    }
    catch {
      case cause: Throwable =>
        throw new AvroSerializationException(items, cause)
    }
  }

  def read(decoder: BinaryDecoder) = Try {
    val items = new scala.collection.mutable.ArrayBuffer[T]

    def readBlock(): Long = {
      val numItems = (AvroLongIO read decoder).get
      val absNumItems = math abs numItems
      if (numItems < 0L) { val bytesInBlock = (AvroLongIO read decoder).get }
      (0L until absNumItems) foreach { _ => items += avroType.itemType.io.read(decoder).get }
      absNumItems
    }

    var itemsRead = readBlock()
    while (itemsRead != 0L) { itemsRead = readBlock() }
    originalTypeVarargsApply(items).asInstanceOf[S] // a Seq is passed to varargs MethodMirror.apply
  }

}