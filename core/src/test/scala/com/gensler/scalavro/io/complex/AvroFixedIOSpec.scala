package com.gensler.scalavro.io.complex

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import com.gensler.scalavro.types._
import com.gensler.scalavro.types.complex._
import com.gensler.scalavro.error._
import com.gensler.scalavro.util.FixedData

import com.gensler.scalavro.protocol.MD5 // test type

import scala.collection.immutable

import java.io.{ PipedInputStream, PipedOutputStream }

class AvroFixedIOSpec extends FlatSpec with ShouldMatchers {

  val md5Type = AvroType[MD5]
  val io = md5Type.io

  "AvroFixedIO" should "be the AvroTypeIO for AvroFixed" in {
    md5Type.io.isInstanceOf[AvroFixedIO[_]] should be (true)
  }

  it should "read and write instances of FixedData subclasses" in {
    val out = new PipedOutputStream
    val in = new PipedInputStream(out)
    val testBytes = "abcd1234defg5678".getBytes.toIndexedSeq
    md5Type.io.write(MD5(testBytes), out)
    val readResult = md5Type.io.read(in)
    readResult should equal (Success(MD5(testBytes)))
  }

}