package com.gensler.scalavro.io.complex

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import com.gensler.scalavro.types._
import com.gensler.scalavro.types.complex._
import com.gensler.scalavro.error._
import com.gensler.scalavro.io.AvroTypeIO

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }

class AvroJEnumIOSpec extends FlatSpec with ShouldMatchers {

  val enumType = AvroType[JDirection]
  val io = enumType.io

  "AvroJEnumIO" should "be the AvroTypeIO for AvroJEnum" in {
    io.isInstanceOf[AvroJEnumIO[_]] should be (true)
  }

  it should "read and write enumerations" in {
    val out = new ByteArrayOutputStream
    io.write(JDirection.NORTH, out)
    io.write(JDirection.SOUTH, out)
    io.write(JDirection.WEST, out)
    io.write(JDirection.EAST, out)

    val in = new ByteArrayInputStream(out.toByteArray)
    io read in should equal (Success(JDirection.NORTH))
    io read in should equal (Success(JDirection.SOUTH))
    io read in should equal (Success(JDirection.WEST))
    io read in should equal (Success(JDirection.EAST))
  }

}