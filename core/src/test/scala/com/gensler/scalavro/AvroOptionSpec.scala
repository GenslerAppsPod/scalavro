package com.gensler.scalavro

import com.gensler.scalavro.test.AvroSpec
import com.gensler.scalavro.types.AvroType
import org.apache.avro.Schema

class AvroOptionSpec extends AvroSpec {

  "AvroType[Option[String]]" should "have valid schema" in {
    new Schema.Parser().parse(AvroType[Option[String]].schema().toString())
  }

  "AvroType[Option[java.lang.Long]]" should "have valid schema" in {
    new Schema.Parser().parse(AvroType[Option[java.lang.Long]].schema().toString())
  }

}
