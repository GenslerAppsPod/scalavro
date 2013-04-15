# Scalavro

A runtime reflection-based Avro library in Scala.

## Motivation
1. We would rather write Scala classes than Avro IDL.
2. We would like to avoid adding a compilation phase to generate IO bindings.

## Goals
1. To provide an in-memory representation of avro schemas and protocols.
2. To generate avro schemas and protocols automatically from Scala types.
3. To produce flat (.avsc) Avro schema files.
4. To generate Scala bindings for reading and writing Avro-mapped Scala types.
5. Generally, to minimize fuss required to create an Avro-capable Scala application.

## General Information
- Built against Scala 2.10.1 with SBT 0.12.2
- Depends upon [spray-json](https://github.com/spray/spray-json)
- The `io` sub-project depends upon the Apache Java implementation of Avro (Version 1.7.4)

## Type Mapping

### Primitive Types

<table>
  <thead>
  	<tr>
  	  <th>Scala Type</th>
  	  <th>Avro Type</th>
  	</tr>
  </thead>
  <tr>
    <td><code>
      Unit
    </code></td>
    <td><code>
      null
    </code></td>
  </tr>
  <tr>
    <td><code>
      Boolean
    </code></td>
    <td><code>
      boolean
    </code></td>
  </tr>
  <tr>
    <td><code>
      Long
    </code></td>
    <td><code>
      long
    </code></td>
  </tr>
  <tr>
    <td><code>
      Int
    </code></td>
    <td><code>
      int
    </code></td>
  </tr>
  <tr>
    <td><code>
      Double
    </code></td>
    <td><code>
      double
    </code></td>
  </tr>
  <tr>
    <td><code>
      Float
    </code></td>
    <td><code>
      float
    </code></td>
  </tr>
  <tr>
    <td><code>
      String
    </code></td>
    <td><code>
      string
    </code></td>
  </tr>
  <tr>
    <td><code>
      scala.collection.Seq[Byte]
    </code></td>
    <td><code>
      bytes
    </code></td>
  </tr>
</table>

### Complex Types

<table>
  <thead>
  	<tr>
  	  <th>Scala Type</th>
  	  <th>Avro Type</th>
  	</tr>
  </thead>
  <tr>
    <td><code>
      scala.collection.Seq[T]
    </code></td>
    <td><code>
      array
    </code></td>
  </tr>
  <tr>
    <td><code>
      scala.collection.Map[String, T]
    </code></td>
    <td><code>
      map
    </code></td>
  </tr>
  <tr>
    <td><code>
      scala.Enumeration#Value
    </code></td>
    <td><code>
      enum
    </code></td>
  </tr>
  <tr>
    <td>
      <code>enum</code> (Java)</td>
    <td><code>
      enum
    </code></td>
  </tr>
  <tr>
    <td><code>
      scala.util.Either[A, B]
    </code></td>
    <td><code>
      union
    </code></td>
  </tr>
  <tr>
    <td><code>
      scala.util.Option[T]
    </code></td>
    <td><code>
      union
    </code></td>
  </tr>
  <tr>
    <td><code>
      com.gensler.scalavro.util.Union[U]
    </code></td>
    <td><code>
      union
    </code></td>
  </tr>
  <tr>
    <td><em>
      Supertypes of non-recursive case classes without type parameters
    </em></td>
    <td><code>
      union
    </code></td>
  </tr>
  <tr>
    <td><em>
      Non-recursive case classes without type parameters
    </em></td>
    <td><code>
      record
    </code></td>
  </tr>
</table>

## Usage Overview: Schema Generation

### Arrays

    import com.gensler.scalavro.types.AvroType
    AvroType[Seq[String]].schema

Which yields:

    {
      "type" : "array",
      "items" : "string"
    }

### Maps

    import com.gensler.scalavro.types.AvroType
    AvroType[Map[String, Double]].schema

Which yields:

    {
      "type" : "map",
      "values" : "double"
    }

### Enums

#### scala.Enumeration

    package com.gensler.scalavro.tests
    import com.gensler.scalavro.types.AvroType

    object CardinalDirection extends Enumeration {
      type CardinalDirection = Value
      val N, NE, E, SE, S, SW, W, NW = Value
    }

    import CardinalDirection._
    AvroType[CardinalDirection].schema

Which yields:

    {
      "name" : "CardinalDirection",
      "type" : "enum",
      "symbols" : ["N","NE","E","SE","S","SW","W","NW"],
      "namespace" : "com.gensler.scalavro.tests.CardinalDirection"
    }

#### Java `enum`

Definition (Java):

    package com.gensler.scalavro.tests;
    enum JCardinalDirection { N, NE, E, SE, S, SW, W, NW };

Use (Scala):

    import com.gensler.scalavro.types.AvroType

    import com.gensler.scalavro.tests.JCardinalDirection
    AvroType[JCardinalDirection].schema

Which yields:

    {
      "name" : "JCardinalDirection",
      "type" : "enum",
      "symbols" : ["N","NE","E","SE","S","SW","W","NW"],
      "namespace" : "com.gensler.scalavro.tests"
    }

### Unions

#### scala.Either

    package com.gensler.scalavro.tests
    import com.gensler.scalavro.types.AvroType

    AvroType[Either[Int, Boolean]].schema

Which yields:

    ["int", "boolean"]

and

    AvroType[Either[Seq[Double], Map[String, Seq[Int]]]].schema

Which yields:

    [{
      "type" : "array",
      "items" : "double"
    },
    {
      "type" : "map",
      "values" : {
        "type" : "array",
        "items" : "int"
      }
    }]

#### scala.Option

    package com.gensler.scalavro.tests
    import com.gensler.scalavro.types.AvroType

    AvroType[Option[String]].schema

Which yields:

    ["string", "null"]

#### com.gensler.scalavro.util.Union.union

    import com.gensler.scalavro.util.Union._
    AvroType[union [Int] #or [String] #or [Boolean]].schema

Which yields:

    ["int", "string", "boolean"]

### Supertypes of case classes

Given:

    class Alpha
    abstract class Beta extends Alpha
    case class Gamma() extends Alpha
    case class Delta() extends Beta
    case class Epsilon[T]() extends Beta

Usage:

    import com.gensler.scalavro.AvroType
    AvroType[Alpha].schema

Which yields:

    [
      {
        "name" : "Delta",
        "type" : "record",
        "fields" : [],
        "namespace" : "com.gensler.scalavro.tests"
      },
      {
        "name" : "Gamma",
        "type" : "record",
        "fields" : [],
        "namespace" : "com.gensler.scalavro.tests"
      }
    ]

Note that in the above example:

- `Beta` is excluded from the union because it is not a case class
- `Epsilon` is excluded from the union because it takes type parameters

### Records

    package com.gensler.scalavro.tests
    import com.gensler.scalavro.types.AvroType

    case class Person(name: String, age: Int)

    val personAvroType = AvroType[Person]
    personAvroType.schema

Which yields:

    {
      "name": "Person",
      "type": "record",
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
      ],
      "namespace": "com.gensler.scalavro.tests"
    }

And perhaps more interestingly:

    case class SantaList(nice: Seq[Person], naughty: Seq[Person])

    val santaListAvroType = AvroType[SantaList]
    santaListAvroType.schema

Which yields:

    {
      "name": "SantaList",
      "type": "record",
      "fields": [
        {
          "name": "nice",
          "type": {"type": "array", "items": "Person"}
        },
        {
          "name": "naughty",
          "type": {"type": "array", "items": "Person"}
        }
      ],
      "namespace": "com.gensler.scalavro.tests"
    }

## Usage Overview: Binary IO

    import com.gensler.scalavro.AvroType
    import com.gensler.scalavro.io.AvroTypeIO
    import com.gensler.scalavro.io.AvroTypeIO.Implicits._
    import scala.util.{Try, Success, Failure}

    case class Person(name: String, age: Int)
    case class SantaList(nice: Seq[Person], naughty: Seq[Person])

    val santaList = SantaList(
      nice = Seq(
        Person("John", 17),
        Person("Eve", 3)
      ),
      naughty = Seq(
        Person("Jane", 25),
        Person("Alice", 65)
      )
    )

    val santaListType = AvroType[SantaList]
    val santaListIO = santaListType.io // implicitly: AvroTypeIO[SantaList]

    val outStream: java.io.OutputStream = // some stream...

    santaListIO.write(santaList, outStream)

    val inStream: java.io.InputStream = // some stream...

    santaListIO.read(inStream) match {
      case Success(readResult) => // readResult is an instance of SantaList
      case Failure(cause)      => // handle failure...
    }

## Current Capabilities
- Schema generation
- Type-safe Avro protocol definitions and JSON output
- Robustness in the face of cyclic type dependencies (such records are never valid Avro)
- Schema conversion to "Parsing Canonical Form"
- Convenient binary IO

## Current Limitations
- Complex types are incomplete:
  - `fixed` is not yet supported
  - binary IO for union types is not yet supported
- JSON IO is not yet implemented.

## Reference
1. [Current Apache Avro Specification](http://avro.apache.org/docs/current/spec.html)
1. [Scala 2.10 Reflection Overview](http://docs.scala-lang.org/overviews/reflection/overview.html)
1. [Great article on schema evolution in various serialization systems](http://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protocol-buffers-thrift.html)
1. [Wickedly clever technique for representing unboxed union types, proposed by Miles Sabin](http://chuusai.com/2011/06/09/scala-union-types-curry-howard)

## Legal
Apache Avro is a trademark of The Apache Software Foundation.

Scalavro is distributed under the BSD 2-Clause License, the text of which follows:

Copyright (c) 2013, Gensler  
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

- Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
