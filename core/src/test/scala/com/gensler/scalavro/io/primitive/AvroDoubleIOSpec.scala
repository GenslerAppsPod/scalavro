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

class AvroDoubleIOSpec extends FlatSpec with ShouldMatchers {

  val io = AvroDoubleIO

  "AvroDoubleIO" should "be the AvroTypeIO for AvroDouble" in {
    val avroTypeIO: AvroTypeIO[_] = AvroDouble.io
    avroTypeIO should be (io)
  }

  it should "read and write Doubles" in {
    val out = new ByteArrayOutputStream

    io.write(math.Pi, out)
    io.write(1.23, out)
    io.write(-1500.123, out)

    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io read in should equal (Success(math.Pi))
    io read in should equal (Success(1.23))
    io read in should equal (Success(-1500.123))
  }

}