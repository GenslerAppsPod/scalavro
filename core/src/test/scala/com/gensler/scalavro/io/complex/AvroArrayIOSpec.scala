package com.gensler.scalavro.io.complex

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import com.gensler.scalavro.types._
import com.gensler.scalavro.types.complex._
import com.gensler.scalavro.error._

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }

class AvroArrayIOSpec extends FlatSpec with ShouldMatchers {

  val intArrayType = AvroType[Seq[Int]]
  val io = intArrayType.io

  "AvroArrayIO" should "be the AvroTypeIO for AvroArray" in {
    io.isInstanceOf[AvroArrayIO[_, _]] should be (true)
  }

  it should "read and write arrays" in {
    val s1 = (0 to 1000).toSeq

    val out = new ByteArrayOutputStream
    io.write(s1, out)

    val in = new ByteArrayInputStream(out.toByteArray)
    io read in should equal (Success(s1))
  }

  it should "return properly typed Seq subtypes when reading" in {
    import scala.collection.mutable.ArrayBuffer
    val avroArrayBuffer = AvroType[ArrayBuffer[String]]
    val avroArrayBufferIO = avroArrayBuffer.io

    val strings = ArrayBuffer("Aa", "Bb", "Cc", "Dd", "Ee")

    val out = new ByteArrayOutputStream
    avroArrayBufferIO.write(strings, out)

    val in = new ByteArrayInputStream(out.toByteArray)
    val Success(readResult) = avroArrayBufferIO read in

    readResult should equal (strings)
    readResult.isInstanceOf[ArrayBuffer[_]] should be (true)
  }

}