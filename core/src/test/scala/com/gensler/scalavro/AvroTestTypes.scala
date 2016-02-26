package com.gensler.scalavro.test

import com.gensler.scalavro.util.FixedData

import scala.collection.immutable

// for testing AvroRecord
case class Person(name: String, age: Int)
case class SantaList(nice: Seq[Person], naughty: Seq[Person])

// cyclic dependency to test dependency robustness
case class A(b: B)
case class B(a: A)

// types for protocol testing
case class Greeting(message: String)
case class Curse(message: String)

abstract class Alpha { def magic: Double }

class Beta extends Alpha { val magic = math.Pi }

case class Gamma(magic: Double) extends Alpha

case class Delta() extends Beta

case class Epsilon[T]() extends Beta

// case class AlphaWrapper(inner: Alpha) extends Alpha { def magic = inner.magic }

class AlphaCollection(as: Seq[Alpha]) extends Seq[Alpha] {
  def apply(idx: Int): Alpha = as apply idx
  def iterator: Iterator[Alpha] = as.iterator
  def length: Int = as.length
  def magic = length.toDouble
}
object AlphaCollection {
  def apply(as: Alpha*): AlphaCollection = new AlphaCollection(as)
}

// good fixed data example
@FixedData.Length(16)
case class MD5(override val bytes: immutable.Seq[Byte])
  extends FixedData(bytes)

// bad fixed data example
@FixedData.Length(4)
case class BadFixed[T](override val bytes: immutable.Seq[Byte])
  extends FixedData(bytes)

case class BoolOrDoubleWrapper(inner: Either[Boolean, Double])

// a recursively defined type
case class SinglyLinkedStringList(data: String, next: Option[SinglyLinkedStringList])

// a case class with default values
case class Exclamation(volume: Int, word: String = "Eureka!")

//sealed trait enum
sealed trait Color
object Color {
  case object Black extends Color
  case object White extends Color
  case object Blue extends Color
}

case class Car(name: String, color: Color)
case class Avatar(image: Array[Byte])

object Direction extends Enumeration {
  type Direction = Value
  val NORTH, EAST, SOUTH, WEST = Value
}
