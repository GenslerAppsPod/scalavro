package com.gensler.scalavro.io.primitive

import java.nio.ByteBuffer

import com.gensler.scalavro.test.{ Avatar, read, write, toHex }
import org.apache.avro.Schema.Parser
import org.scalatest.{ FlatSpec, Matchers }

class AvroByteArrayIOSpec extends FlatSpec with Matchers {

  val avatarSchema = new Parser().parse(
    """
      |{
      |  "name":"com.gensler.scalavro.test.Avatar",
      |  "type":"record",
      |  "fields":[
      |    {
      |      "name":"image",
      |      "type":"bytes"
      |    }
      |  ]
      |}
    """.stripMargin)

  it should "read bytes" in {
    val data = write(avatarSchema) { record =>
      record.put("image", ByteBuffer.wrap("image-data".getBytes))
    }

    val avatar = read[Avatar](data)
    toHex(avatar.image) should equal(toHex("image-data".getBytes))
  }

  it should "write bytes" in {
    val data = write(Avatar("image-data".getBytes))
    val record = read(data, avatarSchema)
    toHex(record.get("image").asInstanceOf[ByteBuffer].array()) should equal(toHex("image-data".getBytes))
  }

}