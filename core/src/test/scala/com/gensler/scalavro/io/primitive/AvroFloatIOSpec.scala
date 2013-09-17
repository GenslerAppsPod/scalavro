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

class AvroFloatIOSpec extends FlatSpec with ShouldMatchers {

  val io = AvroFloatIO

  "AvroFloatIO" should "be the AvroTypeIO for AvroFloat" in {
    val avroTypeIO: AvroTypeIO[_] = AvroFloat.io
    avroTypeIO should be (io)
  }

  it should "read and write Floats" in {
    val out = new ByteArrayOutputStream

    io.write(5.3F, out)
    io.write(-88.421F, out)

    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(5.3F))
    io read in should equal (Success(-88.421F))
  }

}