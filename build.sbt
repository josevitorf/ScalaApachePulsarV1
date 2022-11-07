ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

val pulsar4sVersion = "2.7.3"

lazy val pulsar4s       = "com.sksamuel.pulsar4s" %% "pulsar4s-core"  % pulsar4sVersion
lazy val pulsar4sCirce  = "com.sksamuel.pulsar4s" %% "pulsar4s-circe" % pulsar4sVersion

lazy val root = (project in file("."))
  .settings(
    name := "ScalaApachePulsarV1"
  )

libraryDependencies ++= Seq(
  pulsar4s, pulsar4sCirce
)