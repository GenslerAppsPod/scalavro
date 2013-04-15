package com.gensler.scalavro.tests

import scala.util.{Try, Success, Failure}
import scala.reflect.runtime.universe._

import com.gensler.scalavro.types._
import com.gensler.scalavro.types.complex.AvroUnion
import com.gensler.scalavro.error._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class AvroUnionSpec extends AvroSpec {

  val u1 = AvroType[Either[String, Int]]

  "AvroUnion" should "be parameterized with its corresponding Scala type" in {
    u1 match {
      case avroType: AvroUnion[_] => {

        println(avroType.union.typeMembers)

        avroType.union.contains[String] should be (true)
        avroType.union.contains[Int] should be (true)
      }
      case _ => fail
    }
  }

  it should "be a complex AvroType" in {
    u1.isPrimitive should be (false)
  }

}
