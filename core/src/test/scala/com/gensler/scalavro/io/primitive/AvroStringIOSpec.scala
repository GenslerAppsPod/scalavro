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

class AvroStringIOSpec extends FlatSpec with ShouldMatchers {

  val io = AvroStringIO

  "AvroStringIO" should "be the AvroTypeIO for AvroString" in {
    val avroTypeIO: AvroTypeIO[_] = AvroString.io
    avroTypeIO should be (io)
  }

  it should "read and write Strings" in {
    val text = "The quick brown fox jumped over the lazy dog."

    val out = new ByteArrayOutputStream
    io.write(text, out)

    val bytes = out.toByteArray
    val in = new ByteArrayInputStream(bytes)

    io.read(in).get should equal (text)
  }

}