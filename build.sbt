import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.8",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "SparkMongoDynamo",
    libraryDependencies ++= Dependencies.core ++ Dependencies.scalaSpecs,
    mainClass in assembly := Some("example.LetterCountDynamo"),
    assemblyJarName in assembly := "assignment5.jar",
    test in assembly := {}
  )
