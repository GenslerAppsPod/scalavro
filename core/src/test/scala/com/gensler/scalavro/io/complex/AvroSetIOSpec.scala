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

class AvroSetIOSpec extends FlatSpec with ShouldMatchers {

  val setType = AvroType[Set[Person]]
  val io = setType.io

  "AvroSetIO" should "be the AvroTypeIO for AvroSet" in {
    io.isInstanceOf[AvroSetIO[_, _]] should be (true)
  }

  it should "read and write sets" in {
    val out = new PipedOutputStream
    val in = new PipedInputStream(out)

    val s1 = Set(
      Person("Russel", 45),
      Person("Whitehead", 53),
      Person("Wittgenstein", 22),
      Person("Godel", 37),
      Person("Church", 17),
      Person("Turing", 24)
    )

    io.write(s1, out)
    io read in should equal (Success(s1))
  }

  it should "return properly typed Set subtypes when reading" in {
    val out = new PipedOutputStream
    val in = new PipedInputStream(out)

    import scala.collection.immutable.ListSet
    val listSetType = AvroType[ListSet[Int]]
    val listSetIO = listSetType.io

    val numbers = ListSet(1, 3, 2, 5, 8, 9, 11)

    listSetIO.write(numbers, out)
    val Success(readResult) = listSetIO read in

    readResult should equal (numbers)
    readResult.isInstanceOf[ListSet[_]] should be (true)
  }

}