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

class AvroByteIOSpec extends FlatSpec with ShouldMatchers {

  val io = AvroByteIO

  "AvroByteIO" should "be the AvroTypeIO for AvroByte" in {
    val avroTypeIO: AvroTypeIO[_] = AvroByte.io
    avroTypeIO should be (io)
  }

  it should "read and write Bytes" in {
    val out = new ByteArrayOutputStream

    io.write(5.toByte, out)
    io.write(2.toByte, out)

    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(5.toByte))
    io read in should equal (Success(2.toByte))
  }

}