package com.gensler.scalavro.io.complex

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.gensler.scalavro.test.Person
import com.gensler.scalavro.types.AvroType
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Success

class SchemaResolutionSpec extends FlatSpec with Matchers {

  def toBytes(record: GenericData.Record) = {

    val datumWriter = new GenericDatumWriter[GenericRecord](record.getSchema())
    val out = new ByteArrayOutputStream()
    val encoder = new EncoderFactory().binaryEncoder(out, null)
    datumWriter.write(record, encoder)
    encoder.flush()
    out.toByteArray
  }

  def toHex(data: Array[Byte]) = {
    data.map(b => String.format("%02X ", java.lang.Byte.valueOf(b))).mkString
  }

  it should "handle reordered fields" in {
    val name = "test name"
    val age = 56
    val WriterSchema = new Parser().parse(
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
      val p = new Record(WriterSchema)
      p.put("age", age)
      p.put("name", name)
      p
    })
    val personType = AvroType[Person]

    val Success(person) = personType.io.read(new ByteArrayInputStream(data), Option(WriterSchema))
    person.name should equal(name)
    person.age should equal(age)
  }

}
