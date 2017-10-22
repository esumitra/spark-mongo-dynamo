import sbt._

object Dependencies {
  lazy val scalaSpecs = Seq(
    "org.specs2" %% "specs2-core" % "3.9.5" % "test",
    "org.scalacheck" %% "scalacheck" % "1.13.4" % "test"
  )

  lazy val core = Seq(
    "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
    "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.0",
    "com.amazon.emr" % "emr-dynamodb-hadoop" % "4.2.0" % "provided" exclude("com.fasterxml.jackson.core", "jackson-core") exclude("com.fasterxml.jackson.core", "jackson-databind"),
    "com.typesafe" % "config" % "1.3.1"
  )
}
