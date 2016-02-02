
resolvers += "Spray Micro-Repository" at "http://repo.spray.io/"

libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-xml" % Common.SCALA_XML_VERSION,
      "io.spray" %% "spray-json" % Common.SPRAY_JSON_VERSION,
      "org.apache.avro" % "avro" % Common.AVRO_VERSION
    )

libraryDependencies += "com.jayway.jsonpath" % "json-path" % "2.0.0" % "test"

libraryDependencies += "org.gnieh" %% "diffson" % "1.0.0" % "test"
