package com.gensler.scalavro.test

import com.gensler.scalavro.types.AvroType
import com.gensler.scalavro.types.complex.AvroRecord
import gnieh.diffson.JsonDiff

import scala.collection.mutable
import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

package recursive {
  abstract class A { def n: Double }
  class B extends A { val n = math.Pi }
  case class C(a: A) extends A { def n = a.n }
}

class RecursiveRecordSpec extends AvroSpec {

  "The AvroType object" should "create an AvroType for recursive case classes" in {
    val Success(listAvroType) = AvroType.fromType[SinglyLinkedStringList]
    listAvroType.isInstanceOf[AvroRecord[SinglyLinkedStringList]] should equal (true)
    typeOf[listAvroType.scalaType] =:= typeOf[SinglyLinkedStringList] should be (true)
  }

  it should "produce a schema for a record derived from a recursively defined case class" in {
    JsonDiff.diff(AvroType[SinglyLinkedStringList].schema.toString, """
{
  "name": "SinglyLinkedStringList",
  "namespace": "com.gensler.scalavro.test",
  "type": "record",
  "fields": [{
    "name": "data",
    "type": ["null","string"]
  }, {
    "name": "next",
    "type": ["null", ["com.gensler.scalavro.test.SinglyLinkedStringList", {
      "name": "Reference",
      "namespace": "com.gensler.scalavro",
      "type": "record",
      "fields": [{
        "name": "id",
        "type": "long"
      }]
    }]]
  }]
}
""").ops shouldBe empty
  }

  it should "compute dependentNamedTypes for a record derived from a recursively defined type" in {
    val aType = AvroType[recursive.A]
    val cType = AvroType[recursive.C]

    aType.dependentNamedTypes should equal (Seq(cType))
    cType.dependentNamedTypes should equal (Seq(cType))
  }

}