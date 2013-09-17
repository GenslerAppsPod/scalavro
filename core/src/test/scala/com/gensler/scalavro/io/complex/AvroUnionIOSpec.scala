package com.gensler.scalavro.io.complex

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import com.gensler.scalavro.types._
import com.gensler.scalavro.types.complex._
import com.gensler.scalavro.error._
import com.gensler.scalavro.util.Union._

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }

// mock types:
case class BoolOrDoubleWrapper(inner: Either[Boolean, Double])

abstract class Alpha { def magic: Double }
class Beta extends Alpha { val magic = math.Pi }
case class Gamma(magic: Double) extends Alpha
case class Delta() extends Beta
case class Epsilon[T]() extends Beta
// end mock types

class AvroUnionIOSpec extends FlatSpec with ShouldMatchers {

  type ISB = union[Int]#or[String]#or[Boolean]

  val unionType = AvroType[Either[Int, String]]
  val io = unionType.io

  "AvroUnionIO" should "be the AvroTypeIO for AvroUnion" in {
    io.isInstanceOf[AvroUnionIO[_, _]] should be (true)
  }

  it should "read and write union members derived from scala.Either" in {
    val out = new ByteArrayOutputStream
    io.write(Right("Hello"), out)
    io.write(Left(55), out)

    val in = new ByteArrayInputStream(out.toByteArray)
    (io read in).get should equal (Right("Hello"))
    (io read in).get should equal (Left(55))
  }

  it should "read and write union members derived from scala.Option" in {
    val optionType = AvroType[Option[String]]

    val out = new ByteArrayOutputStream
    optionType.io.write(Some("Hello from option"), out)
    optionType.io.write(None, out)

    val in = new ByteArrayInputStream(out.toByteArray)
    (optionType.io read in).get should equal (Some("Hello from option"))
    (optionType.io read in).get should equal (None)
  }

  it should "read and write union members derived from bare Unions" in {
    val bareIO = AvroType[ISB].io.asInstanceOf[AvroBareUnionIO[ISB, ISB]]

    val out = new ByteArrayOutputStream

    import org.apache.avro.io.EncoderFactory
    val encoder = EncoderFactory.get.directBinaryEncoder(out, null)

    bareIO.writeBare(555, encoder)
    bareIO.writeBare(false, encoder)
    bareIO.writeBare("Unboxed unions!", encoder)

    val in = new ByteArrayInputStream(out.toByteArray)
    (bareIO read in).get should equal (555)
    (bareIO read in).get should equal (false)
    (bareIO read in).get should equal ("Unboxed unions!")
  }

  it should "read and write union members derived from class hierarchies" in {
    val classUnion = AvroType[Alpha].io

    val first = Delta()
    val second: Alpha = Gamma(123.45)

    val out = new ByteArrayOutputStream
    classUnion.write(first, out)
    classUnion.write(second, out)

    val in = new ByteArrayInputStream(out.toByteArray)
    (classUnion read in).get should equal (first)
    (classUnion read in).get should equal (second)
  }

  it should "read and write case classes with union parameters" in {
    val wrapperType = AvroType[BoolOrDoubleWrapper].io
    val boolOrDouble = BoolOrDoubleWrapper(Left(true))

    val out = new ByteArrayOutputStream
    wrapperType.write(boolOrDouble, out)

    val in = new ByteArrayInputStream(out.toByteArray)
    (wrapperType read in).get should equal (boolOrDouble)
  }

  it should "read and write arrays of unions" in {
    val unionArrayType = AvroType[Seq[Either[Int, String]]].io
    val mixed: Seq[Either[Int, String]] = Seq(
      Left(55),
      Right("Hello"),
      Left(110),
      Right("World")
    )

    val out = new ByteArrayOutputStream
    unionArrayType.write(mixed, out)

    val in = new ByteArrayInputStream(out.toByteArray)
    (unionArrayType read in).get should equal (mixed)
  }

  it should "read and write maps of simple unions" in {
    val unionMapType = AvroType[Map[String, Either[Int, String]]].io

    val map: Map[String, Either[Int, String]] = Map(
      "uno" -> Left(55),
      "due" -> Right("Hello"),
      "tre" -> Left(110),
      "quattro" -> Right("World")
    )

    val out = new ByteArrayOutputStream
    unionMapType.write(map, out)

    val in = new ByteArrayInputStream(out.toByteArray)
    (unionMapType read in).get should equal (map)
  }

}