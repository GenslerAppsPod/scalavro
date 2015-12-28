package com.gensler.scalavro.io.complex

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }
import java.nio.ByteBuffer

import com.gensler.scalavro.test.Person
import com.gensler.scalavro.types.AvroType
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{ GenericData, GenericDatumWriter, GenericRecord }
import org.apache.avro.io.EncoderFactory
import org.scalatest.{ FlatSpec, Matchers }

import scala.util.{ Failure, Success }

case class WithDefaultValue(a: String, b: String = "default")

case class NumberLong(value: Long)
case class NumberFloat(value: Float)
case class NumberDouble(value: Double)

case class StringField(text: String)
case class BytesField(data: Seq[Byte])

class SchemaResolutionSpec extends FlatSpec with Matchers {

  def toBytes(record: GenericData.Record) = {

    val datumWriter = new GenericDatumWriter[GenericRecord](record.getSchema())
    val out = new ByteArrayOutputStream()
    val encoder = new EncoderFactory().binaryEncoder(out, null)
    datumWriter.write(record, encoder)
    encoder.flush()
    out.toByteArray
  }

  it should "handle reordered fields" in {
    val name = "test name"
    val age = 56
    val writerSchema = new Parser().parse(
      """
        |{
        | "name":"com.gensler.scalavro.test.Person",
        | "type":"record",
        | "fields":[
        |   {
        |     "name":"age",
        |     "type":"int"
        |   },
        |   {
        |     "name":"name",
        |     "type":["null","string"]
        |   }
        | ]
        |}
      """.stripMargin)
    val data = toBytes({
      val p = new Record(writerSchema)
      p.put("age", age)
      p.put("name", name)
      p
    })
    val personType = AvroType[Person]

    val Success(person) = personType.io.read(new ByteArrayInputStream(data), writerSchema)
    person.name should equal(name)
    person.age should equal(age)
  }

  it should "ignore field when the writer's record contains a field with a name not present in the reader's record" in {
    val name = "test name"
    val age = 56
    val writerSchema = new Parser().parse(
      """
        |{
        | "name":"com.gensler.scalavro.test.Person",
        | "type":"record",
        | "fields":[
        |   {
        |     "name":"age",
        |     "type":"int"
        |   },
        |   {
        |     "name":"to_ignore",
        |     "type":"int"
        |   },
        |   {
        |     "name":"name",
        |     "type":["null","string"]
        |   }
        | ]
        |}
      """.stripMargin)
    val data = toBytes({
      val p = new Record(writerSchema)
      p.put("age", age)
      p.put("to_ignore", 12)
      p.put("name", name)
      p
    })
    val personType = AvroType[Person]

    val Success(person) = personType.io.read(new ByteArrayInputStream(data), writerSchema)
    person.name should equal(name)
    person.age should equal(age)
  }

  it should "use default when missing" in {
    val writerSchema = new Parser().parse(
      """
        |{
        | "name":"WithDefaultValue",
        | "type":"record",
        | "fields":[
        |   {
        |     "name":"a",
        |     "type":["null","string"]
        |   }
        | ]
        |}
      """.stripMargin)
    val data = toBytes({
      val p = new Record(writerSchema)
      p.put("a", "test value")
      p
    })

    val Success(record) = AvroType[WithDefaultValue].io.read(new ByteArrayInputStream(data), writerSchema)
    record.a should equal("test value")
    record.b should equal("default")
  }

  val testInt = 89
  def checkIntPromotion[T] = checkNumberPromotion[T]("int", testInt) _

  val testLong = 97L
  def checkLongPromotion[T] = checkNumberPromotion[T]("long", testLong) _

  val testFloat = 97.78F
  def checkFloatPromotion[T] = checkNumberPromotion[T]("float", testFloat) _

  def checkNumberPromotion[T](typeName: String, testValue: Any)(readerAvroType: AvroType[T], assertion: (T) => Unit) = {
    val writerSchema = new Parser().parse(
      s"""
        |{
        | "name":"Number",
        | "type":"record",
        | "fields":[
        |   {
        |     "name":"value",
        |     "type":"${typeName}"
        |   }
        | ]
        |}
      """.stripMargin)
    val data = toBytes({
      val p = new Record(writerSchema)
      p.put("value", testValue)
      p
    })

    val result = readerAvroType.io.read(new ByteArrayInputStream(data), writerSchema)
    result match {
      case Success(record) => assertion(record)
      case Failure(ex)     => fail(ex)
    }
  }

  it should "promote int to long" in {
    checkIntPromotion(AvroType[NumberLong], (record: NumberLong) => {
      record.value shouldBe a[java.lang.Long]
      record.value should equal(testInt)
    })
  }

  it should "promote int to float" in {
    checkIntPromotion(AvroType[NumberFloat], (record: NumberFloat) => {
      record.value shouldBe a[java.lang.Float]
      record.value should equal(testInt)
    })
  }

  it should "promote int to double" in {
    checkIntPromotion(AvroType[NumberDouble], (record: NumberDouble) => {
      record.value shouldBe a[java.lang.Double]
      record.value should equal(testInt)
    })
  }

  it should "promote long to double" in {
    checkLongPromotion(AvroType[NumberDouble], (record: NumberDouble) => {
      record.value shouldBe a[java.lang.Double]
      record.value should equal(testLong)
    })
  }

  it should "promote long to float" in {
    checkLongPromotion(AvroType[NumberFloat], (record: NumberFloat) => {
      record.value shouldBe a[java.lang.Float]
      record.value should equal(testLong)
    })
  }

  it should "promote float to double" in {
    checkFloatPromotion(AvroType[NumberDouble], (record: NumberDouble) => {
      record.value shouldBe a[java.lang.Double]
      record.value should equal(testFloat)
    })
  }

  it should "promote string to nullable string" in {
    val writerSchema = new Parser().parse(
      """
        |{
        | "name":"StringField",
        | "type":"record",
        | "fields":[
        |   {
        |     "name":"text",
        |     "type":"string"
        |   }
        | ]
        |}
      """.stripMargin)
    val data = toBytes({
      val p = new Record(writerSchema)
      p.put("text", "test value")
      p
    })

    val Success(record) = AvroType[StringField].io.read(new ByteArrayInputStream(data), writerSchema)
    record.text should equal("test value")
  }

  it should "promote bytes to nullable string" in {
    val writerSchema = new Parser().parse(
      """
        |{
        | "name":"StringField",
        | "type":"record",
        | "fields":[
        |   {
        |     "name":"text",
        |     "type":"bytes"
        |   }
        | ]
        |}
      """.stripMargin)
    val data = toBytes({
      val p = new Record(writerSchema)
      p.put("text", ByteBuffer.wrap("test value".getBytes("UTF-8")))
      p
    })

    val Success(record) = AvroType[StringField].io.read(new ByteArrayInputStream(data), writerSchema)
    record.text should equal("test value")
  }

  it should "promote string to bytes" in {
    val writerSchema = new Parser().parse(
      """
        |{
        | "name":"BytesField",
        | "type":"record",
        | "fields":[
        |   {
        |     "name":"data",
        |     "type":"string"
        |   }
        | ]
        |}
      """.stripMargin)
    val data = toBytes({
      val p = new Record(writerSchema)
      p.put("data", "test data")
      p
    })

    val Success(record) = AvroType[BytesField].io.read(new ByteArrayInputStream(data), writerSchema)
    new String(record.data.toArray, "UTF-8") should equal("test data")
  }

}
