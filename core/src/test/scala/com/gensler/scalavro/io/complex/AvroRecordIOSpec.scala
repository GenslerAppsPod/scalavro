package com.gensler.scalavro.io.complex

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import com.gensler.scalavro.types._
import com.gensler.scalavro.types.complex._
import com.gensler.scalavro.error._

import com.gensler.scalavro.io.AvroTypeIO

import java.io.{
  PipedInputStream,
  PipedOutputStream
}

// for testing
case class Person(name: String, age: Int)

// for testing
case class SantaList(nice: Seq[Person], naughty: Seq[Person])

class AvroRecordIOSpec extends FlatSpec with ShouldMatchers {

  "AvroRecordIO" should "be the AvroTypeIO for AvroRecord" in {
    val personIO = AvroType[Person].io
    val avroTypeIO: AvroTypeIO[_] = personIO
    avroTypeIO should equal (personIO)
  }

  it should "read and write simple records" in {
    val out = new PipedOutputStream
    val in = new PipedInputStream(out)

    val personIO = AvroType[Person].io

    val julius = Person("Julius Caesar", 2112)

    personIO.write(julius, out)
    personIO read in should equal (Success(julius))
  }

  it should "read and write complex record instances" in {
    val out = new PipedOutputStream
    val in = new PipedInputStream(out)

    val santaListIO = AvroType[SantaList].io

    val sList = SantaList(
      nice = Seq(Person("Suzie", 9)),
      naughty = Seq(Person("Tommy", 7), Person("Eve", 3))
    )

    santaListIO.write(sList, out)
    santaListIO read in should equal (Success(sList))
  }

  it should "read and write HandshakeRequest instances" in {
    import com.gensler.scalavro.protocol.{ HandshakeRequest, MD5 }

    val out = new PipedOutputStream
    val in = new PipedInputStream(out)

    val handshakeRequestIO = AvroType[HandshakeRequest].io

    val request = HandshakeRequest(
      clientHash = MD5("abcd1234defg5678".getBytes.toIndexedSeq),
      clientProtocol = Some("{}"), // None,
      serverHash = MD5("abcd1234defg5678".getBytes.toIndexedSeq),
      meta = Some(Map[String, Seq[Byte]]()) // None
    )

    handshakeRequestIO.write(request, out)
    val readResult = (handshakeRequestIO read in).get
    readResult should equal (request)
  }

}