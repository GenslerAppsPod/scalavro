package com.gensler.scalavro.io.complex

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import com.gensler.scalavro.types._
import com.gensler.scalavro.types.complex._
import com.gensler.scalavro.error._

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  PipedInputStream,
  PipedOutputStream
}

class AvroMapIOSpec extends FlatSpec with ShouldMatchers {

  val intMapType = AvroType.fromType[Map[String, Int]].get

  "AvroMapIO" should "be the AvroTypeIO for AvroMap" in {
    intMapType.io.isInstanceOf[AvroMapIO[_, _]] should be (true)
  }

  it should "read and write maps" in {

    val io = intMapType.io

    val m1: Map[String, Int] = Map(
      "uno" -> 1,
      "due" -> 2,
      "tre" -> 3,
      "quattro" -> 4,
      "cinque" -> 5
    )

    val out = new ByteArrayOutputStream
    io.write(m1, out)

    val in = new ByteArrayInputStream(out.toByteArray)
    val Success(readResult) = io read in
    readResult should equal (m1)
    readResult.get("cinque") should be (Some(5))
  }

  it should "read and write maps of bytes" in {

    val io = AvroType[Map[String, Seq[Byte]]].io

    val out = new PipedOutputStream
    val in = new PipedInputStream(out)

    val bytesMap: Map[String, Seq[Byte]] = Map(
      "uno" -> "one".getBytes.toSeq,
      "due" -> "two".getBytes.toSeq,
      "tre" -> "three".getBytes.toSeq,
      "quattro" -> "four".getBytes.toSeq,
      "cinque" -> "five".getBytes.toSeq
    )

    io.write(bytesMap, out)

    val Success(readResult) = io read in

    readResult should equal (bytesMap)
    readResult.get("due") should be (Some("two".getBytes.toSeq))
  }

  it should "return properly typed Map subtypes when reading" in {
    val out = new PipedOutputStream
    val in = new PipedInputStream(out)

    import scala.collection.immutable.ListMap
    val listMapType = AvroType[ListMap[String, Int]]
    val listMapIO = listMapType.io

    val numbers = ListMap("one" -> 1, "two" -> 2, "three" -> 3)

    listMapIO.write(numbers, out)
    val Success(readResult) = listMapIO read in

    readResult should equal (numbers)
    readResult.isInstanceOf[ListMap[_, _]] should be (true)
  }

}