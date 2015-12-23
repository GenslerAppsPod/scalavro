package com.gensler.scalavro.test

import gnieh.diffson.JsonDiff

import scala.collection.mutable
import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

import com.gensler.scalavro.types._
import com.gensler.scalavro.types.primitive._
import com.gensler.scalavro.types.complex._

class AvroTypeSpec extends AvroSpec {

  // primitives
  "The AvroType companion object" should "return valid primitive avro types" in {
    AvroType[Boolean] should be (AvroBoolean)
    AvroType[Byte] should be (AvroByte)
    AvroType[Seq[Byte]] should be (AvroBytes)
    AvroType[Char] should be (AvroChar)
    AvroType[Double] should be (AvroDouble)
    AvroType[Float] should be (AvroFloat)
    AvroType[Int] should be (AvroInt)
    AvroType[Long] should be (AvroLong)
    AvroType[Short] should be (AvroShort)
    AvroType[String] should be (AvroString)
    AvroType[Unit] should be (AvroNull)
    AvroType[scala.xml.Node] should be (AvroXml)

    AvroType[java.lang.Boolean] should be (AvroJavaBoolean)
    AvroType[java.lang.Byte] should be (AvroJavaByte)
    AvroType[java.lang.Character] should be (AvroJavaCharacter)
    AvroType[java.lang.Double] should be (AvroJavaDouble)
    AvroType[java.lang.Float] should be (AvroJavaFloat)
    AvroType[java.lang.Integer] should be (AvroJavaInteger)
    AvroType[java.lang.Long] should be (AvroJavaLong)
    AvroType[java.lang.Short] should be (AvroJavaShort)
    // support org.w3c.dom Node/Element?
  }

  // arrays
  it should "return valid AvroArray types for Arrays" in {
    val avroType = AvroType[Array[Int]]
    avroType.isInstanceOf[AvroJArray[_]] should be (true)
    typeOf[avroType.scalaType] =:= typeOf[Array[Int]] should be (true)
  }

  it should "return valid AvroArray types for Seqs" in {
    AvroType.fromType[Seq[Int]] match {
      case Success(avroType) => {
        avroType.isInstanceOf[AvroArray[_, _]] should be(true)
        typeOf[avroType.scalaType] =:= typeOf[Seq[Int]] should be(true)
      }
      case Failure(cause) => throw cause
    }

    AvroType[IndexedSeq[String]] match {
      case avroType: AvroArray[_, _] => {
        avroType.originalTypeTag.tpe =:= typeOf[IndexedSeq[String]] should be(true)
      }
      case _ => fail
    }

    AvroType[Vector[Float]] match {
      case avroType: AvroArray[_, _] => {
        avroType.originalTypeTag.tpe =:= typeOf[Vector[Float]] should be(true)
      }
      case _ => fail
    }

    import scala.collection.mutable.ArrayBuffer
    AvroType[ArrayBuffer[Boolean]] match {
      case avroType: AvroArray[_, _] => {
        avroType.originalTypeTag.tpe =:= typeOf[ArrayBuffer[Boolean]] should be(true)
      }
      case _ => fail
    }

    AvroType.fromType[Seq[Seq[Seq[Byte]]]] match {
      case Success(avroType) => {
        avroType.isInstanceOf[AvroArray[_, _]] should be(true)
        typeOf[avroType.scalaType] =:= typeOf[Seq[Seq[Seq[Byte]]]] should be(true)
      }
      case Failure(cause) => throw cause
    }
  }
  it should "return valid AvroArray type for seq of records" in {

    val diff = JsonDiff.diff(AvroType[Seq[SantaList]].schema.toString, """
{
  "type": "array",
  "items": [{
    "name": "SantaList",
    "namespace": "com.gensler.scalavro.test",
    "type": "record",
    "fields": [{
      "name": "nice",
      "type": {
        "type": "array",
        "items": [{
          "name": "Person",
          "namespace": "com.gensler.scalavro.test",
          "type": "record",
          "fields": [{
            "name": "name",
            "type": ["null", "string"]
          }, {
            "name": "age",
            "type": "int"
          }]
        }, {
          "name": "Reference",
          "namespace": "com.gensler.scalavro",
          "type": "record",
          "fields": [{
            "name": "id",
            "type": "long"
          }]
        }]
      }
    }, {
      "name": "naughty",
      "type": {
        "type": "array",
        "items": ["com.gensler.scalavro.test.Person", "com.gensler.scalavro.Reference"]
      }
    }]
  }, "com.gensler.scalavro.Reference"]
}
""")

    diff.ops shouldBe empty
  }

  // sets
  it should "return valid AvroArray types for Sets" in {
    AvroType.fromType[Set[Int]] match {
      case Success(avroType) => {
        avroType.isInstanceOf[AvroSet[_, _]] should be (true)
        typeOf[avroType.scalaType] =:= typeOf[Set[Int]] should be (true)
      }
      case Failure(cause) => throw cause
    }
  }

  // maps
  it should "return valid AvroMap types" in {
    AvroType.fromType[Map[String, Int]] match {
      case Success(avroType) => {
        avroType.isInstanceOf[AvroMap[_, _]] should be (true)
        typeOf[avroType.scalaType] =:= typeOf[Map[String, Int]] should be (true)
      }
      case Failure(cause) => throw cause
    }

    AvroType.fromType[Map[String, Seq[Double]]] match {
      case Success(avroType) => {
        avroType.isInstanceOf[AvroMap[_, _]] should be (true)
        typeOf[avroType.scalaType] =:= typeOf[Map[String, Seq[Double]]] should be (true)
      }
      case Failure(cause) => throw cause
    }
  }

  // unions
  it should "return valid AvroUnion types subtypes of Either[A, B]" in {
    AvroType[Either[Double, Int]] match {
      case avroType: AvroUnion[_, _] => {
        avroType.union.contains[Double] should be (true)
        avroType.union.contains[Int] should be (true)
      }
      case _ => fail
    }

    AvroType[Either[Seq[Double], Map[String, Seq[Int]]]] match {
      case avroType: AvroUnion[_, _] => {
        avroType.union.contains[Seq[Double]] should be (true)
        avroType.union.contains[Map[String, Seq[Int]]] should be (true)
      }
      case _ => fail
    }
  }

  it should "return valid AvroUnion types subtypes of Option[T]" in {
    AvroType[Option[String]] match {
      case avroType: AvroUnion[_, _] => {
        avroType.union.contains[String] should be (true)
        avroType.union.contains[Unit] should be (true)
      }
      case _ => fail
    }
  }

  it should "return valid AvroUnion types subtypes of Union.not[A]" in {
    import com.gensler.scalavro.util.Union._
    AvroType[union[Int]#or[String]#or[Boolean]] match {
      case avroType: AvroUnion[_, _] => {
        avroType.union.typeMembers should have size (3)
        avroType.union.contains[Int] should be (true)
        avroType.union.contains[String] should be (true)
        avroType.union.contains[Boolean] should be (true)
      }
      case _ => fail
    }
  }

  it should "return valid AvroUnion types supertypes of avro-typeable classes" in {
    AvroType[Alpha] match {
      case avroType: AvroUnion[_, _] => {
        avroType.union.typeMembers should have size (2)
        avroType.union.contains[Gamma] should be (true)
        avroType.union.contains[Delta] should be (true)
        avroType.union.contains[AlphaCollection] should be (false)
      }
      case _ => fail
    }
  }

  // fixed-length data
  it should "return valid AvroFixed types for subclasses of FixedData" in {
    val md5Type = AvroType[MD5]
    md5Type.isInstanceOf[AvroFixed[_]] should be (true)
    val md5Fixed = md5Type.asInstanceOf[AvroFixed[MD5]]
    md5Fixed.namespace should be (Some("com.gensler.scalavro.test"))
    md5Fixed.name should be ("MD5")
    md5Fixed.fullyQualifiedName should be ("com.gensler.scalavro.test.MD5")
    md5Fixed dependsOn md5Type should be (true)
  }

  // records
  it should "return valid AvroRecord types for product types" in {
    val personType = AvroType[Person]
    val santaListType = AvroType[SantaList]

    personType.isInstanceOf[AvroRecord[_]] should be (true)
    typeOf[personType.scalaType] =:= typeOf[Person] should be (true)
    val personRecord = personType.asInstanceOf[AvroRecord[Person]]
    personRecord.namespace should be (Some("com.gensler.scalavro.test"))
    personRecord.name should be ("Person")
    personRecord.fullyQualifiedName should be ("com.gensler.scalavro.test.Person")
    personType dependsOn personType should be (true)
    personType dependsOn santaListType should be (false)

    santaListType.isInstanceOf[AvroRecord[_]] should be (true)
    typeOf[santaListType.scalaType] =:= typeOf[SantaList] should be (true)
    val santaListRecord = santaListType.asInstanceOf[AvroRecord[SantaList]]
    santaListRecord.namespace should be (Some("com.gensler.scalavro.test"))
    santaListRecord.name should be ("SantaList")
    santaListRecord.fullyQualifiedName should be ("com.gensler.scalavro.test.SantaList")
    santaListType dependsOn personType should be (true)
    santaListType dependsOn santaListType should be (true)
  }

  it should "allow circular dependencies among AvroRecord types" in {

    JsonDiff.diff(AvroType[A].schema.toString, """
{
  "name": "A",
  "namespace": "com.gensler.scalavro.test",
  "type": "record",
  "fields": [{
    "name": "b",
    "type": [{
      "name": "B",
      "namespace": "com.gensler.scalavro.test",
      "type": "record",
      "fields": [{
        "name": "a",
        "type": ["com.gensler.scalavro.test.A", {
          "name": "Reference",
          "namespace": "com.gensler.scalavro",
          "type": "record",
          "fields": [{
            "name": "id",
            "type": "long"
          }]
        }]
      }]
    }, "com.gensler.scalavro.Reference"]
  }]
}
""").ops shouldBe empty

    JsonDiff.diff(AvroType[B].schema.toString, """
{
  "name": "B",
  "namespace": "com.gensler.scalavro.test",
  "type": "record",
  "fields": [{
    "name": "a",
    "type": [{
      "name": "A",
      "namespace": "com.gensler.scalavro.test",
      "type": "record",
      "fields": [{
        "name": "b",
        "type": ["com.gensler.scalavro.test.B", {
          "name": "Reference",
          "namespace": "com.gensler.scalavro",
          "type": "record",
          "fields": [{
            "name": "id",
            "type": "long"
          }]
        }]
      }]
    }, "com.gensler.scalavro.Reference"]
  }]
}
""").ops shouldBe empty
  }

  it should "expose class-level default arguments in generated schemas" in {
    JsonDiff.diff(AvroType[Exclamation].schema.toString, """
{
  "name": "Exclamation",
  "namespace": "com.gensler.scalavro.test",
  "type": "record",
  "fields": [{
    "name": "volume",
    "type": "int"
  }, {
    "name": "word",
    "type": ["null", "string"],
    "default": "Eureka!"
  }]
}
    """).ops shouldBe empty
  }

}