package com.gensler.scalavro.test

import com.gensler.scalavro.types.AvroType
import com.jayway.jsonpath.JsonPath

class AvroSealedTraitEnumSpec extends AvroSpec {

  it should "generate self-contained schema for record" in {
    val colorType = AvroType[Color]
    val json = JsonPath.parse(colorType.selfContainedSchema().toString())
    json.read[String]("$.type") should be ("enum")
    json.read[String]("$.name") should be ("Color")
  }

  it should "generate self-contained schema for record with enum field" in {
    val carType = AvroType[Car]
    val json = JsonPath.parse(carType.selfContainedSchema().toString())
    json.read[String]("$.name") should be ("Car")
    json.read[String]("$.namespace") should be ("com.gensler.scalavro.test")
  }
}
