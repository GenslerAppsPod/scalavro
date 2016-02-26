package com.gensler.scalavro.test

import com.gensler.scalavro.types._
import com.jayway.jsonpath.JsonPath
import org.apache.avro.Schema.Parser

class AvroByteArraySpec extends AvroSpec {

  it should "generate valid schema" in {
    val schema = AvroType[Avatar].schema().toString()
    new Parser().parse(schema)
    val json = JsonPath.parse(schema)
    json.read[String]("$.fields[0].name") should be ("image")
    json.read[String]("$.fields[0].type") should be ("bytes")
  }

}