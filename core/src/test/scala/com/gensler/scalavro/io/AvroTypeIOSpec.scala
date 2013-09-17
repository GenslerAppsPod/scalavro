package com.gensler.scalavro.io.complex

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

import org.scalatest.FlatSpec
import org.scalatest.matchers.{ ShouldMatchers, Matcher, MatchResult }

import com.gensler.scalavro.types._
import com.gensler.scalavro.types.complex._
import com.gensler.scalavro.types.primitive._

import com.gensler.scalavro.io.complex._
import com.gensler.scalavro.io.primitive._

class AvroTypeIOSpec extends FlatSpec with ShouldMatchers {

  it should "resolve AvroTypeIO objects for primitive types implicitly" in {
    AvroBoolean.io should equal (AvroBooleanIO)
    AvroBytes.io should equal (AvroBytesIO)
    AvroDouble.io should equal (AvroDoubleIO)
    AvroFloat.io should equal (AvroFloatIO)
    AvroInt.io should equal (AvroIntIO)
    AvroLong.io should equal (AvroLongIO)
    AvroNull.io should equal (AvroNullIO)
    AvroString.io should equal (AvroStringIO)
  }

}