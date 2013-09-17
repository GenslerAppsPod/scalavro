package com.gensler.scalavro.io.primitive

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import com.gensler.scalavro.types._
import com.gensler.scalavro.types.primitive._
import com.gensler.scalavro.error._

import com.gensler.scalavro.io.AvroTypeIO

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }

class AvroBooleanIOSpec extends FlatSpec with ShouldMatchers {

  val io = AvroBooleanIO

  "AvroBooleanIO" should "be the AvroTypeIO for AvroBoolean" in {
    val avroTypeIO: AvroTypeIO[_] = AvroBoolean.io
    avroTypeIO should be (io)
  }

  it should "write Booleans to a stream" in {
    val out = new ByteArrayOutputStream

    io.write(true, out)
    io.write(false, out)

    val bytes = out.toByteArray
    bytes.toSeq should equal (Seq(1.toByte, 0.toByte))

    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(true))
    io read in should equal (Success(false))
  }

  it should "read Booleans from a stream" in {
    val trueStream = new ByteArrayInputStream(Array(1.toByte))
    val falseStream = new ByteArrayInputStream(Array(0.toByte))

    io read trueStream should equal (Success(true))
    io read falseStream should equal (Success(false))
  }

}