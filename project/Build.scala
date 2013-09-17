import sbt._
import Keys._

import com.typesafe.sbt.SbtPgp._
import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._
import sbtunidoc.Plugin._

object ScalavroBuild extends Build {

//////////////////////////////////////////////////////////////////////////////
// PROJECT INFO
//////////////////////////////////////////////////////////////////////////////

  val ORGANIZATION = "com.gensler"
  val PROJECT_NAME = "scalavro"
  val PROJECT_VERSION = "0.4.0"
  val SCALA_VERSION = "2.10.2"


//////////////////////////////////////////////////////////////////////////////
// DEPENDENCY VERSIONS
//////////////////////////////////////////////////////////////////////////////

  val SPRAY_JSON_VERSION = "1.2.5"
  val AVRO_VERSION = "1.7.5"
  val LOGBACK_VERSION = "1.0.9"
  val SCALATEST_VERSION = "2.0.M5b"
  val REFLECTIONS_VERSION = "0.9.9-RC1"


//////////////////////////////////////////////////////////////////////////////
// ROOT PROJECT
//////////////////////////////////////////////////////////////////////////////

  lazy val root = Project(
    id = PROJECT_NAME,
    base = file("."),
    settings = commonSettings ++ unidocSettings
  ) aggregate(
    core, util
  ) dependsOn (
    core, util
  )


//////////////////////////////////////////////////////////////////////////////
// SUB-PROJECTS
//////////////////////////////////////////////////////////////////////////////

  def subproject(name: String) = "%s-%s".format(PROJECT_NAME, name)

  lazy val core = Project(
    id = subproject("core"),
    base = file("core"),
    settings = commonSettings
  ) settings (
    resolvers += "Spray Micro-Repository" at "http://repo.spray.io/",
    libraryDependencies ++= Seq(
      "io.spray" %% "spray-json" % SPRAY_JSON_VERSION,
      "org.apache.avro" % "avro" % AVRO_VERSION
    )
  ) dependsOn(
    util
  )

  lazy val util = Project(
    id = subproject("util"),
    base = file("util"),
    settings = commonSettings
  ) settings(
    libraryDependencies += "org.reflections" % "reflections" % REFLECTIONS_VERSION
  )


//////////////////////////////////////////////////////////////////////////////
// SHARED SETTINGS
//////////////////////////////////////////////////////////////////////////////

  lazy val commonSettings = Project.defaultSettings ++ basicSettings ++ formatSettings ++ publishSettings

  lazy val basicSettings = Seq(
    version := PROJECT_VERSION,
    organization := ORGANIZATION,
    scalaVersion := SCALA_VERSION,
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % SCALA_VERSION,
      "ch.qos.logback" % "logback-classic" % LOGBACK_VERSION,
      "org.scalatest" %% "scalatest" % SCALATEST_VERSION % "test"
    ),
    scalacOptions in Compile ++= Seq(
      "-unchecked",
      "-deprecation",
      "-feature"
    ),
    parallelExecution in Test := false,
    fork in Test := true
  )

  lazy val publishSettings = Seq(
    publishMavenStyle := true,
    useGpg := true,
    pomIncludeRepository := { _ => false },
    licenses := Seq(
      "BSD-style" -> url("http://opensource.org/licenses/BSD-2-Clause")
    ),
    homepage := Some(url("http://genslerappspod.github.io/scalavro/")),
    pomExtra := (
      <scm>
        <url>git@github.com:GenslerAppsPod/scalavro.git</url>
        <connection>scm:git:git@github.com:GenslerAppsPod/scalavro.git</connection>
      </scm>
      <developers>
        <developer>
          <id>ConnorDoyle</id>
          <name>Connor Doyle</name>
          <url>http://gensler.com</url>
        </developer>
      </developers>
    ),
    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  )

  lazy val formatSettings = scalariformSettings ++ Seq(
    ScalariformKeys.preferences := FormattingPreferences()
      .setPreference(IndentWithTabs, false)
      .setPreference(IndentSpaces, 2)
      .setPreference(AlignParameters, false)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(MultilineScaladocCommentsStartOnFirstLine, false)
      .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, true)
      .setPreference(PreserveDanglingCloseParenthesis, true)
      .setPreference(CompactControlReadability, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(PreserveSpaceBeforeArguments, true)
      .setPreference(SpaceBeforeColon, false)
      .setPreference(SpaceInsideBrackets, false)
      .setPreference(SpaceInsideParentheses, false)
      .setPreference(SpacesWithinPatternBinders, true)
      .setPreference(FormatXml, true)
  )

}
