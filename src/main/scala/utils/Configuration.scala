/**
  * application configuration
  * to connecto to mongo DB, pass in optional map
  * "spark.mongodb.output.uri" -> "mongodb://127.0.0.1/database.collection"
  */

package utils

case class SparkConfiguration(
  name: String,
  masterConf: Option[String],
  props: Option[Map[String, String]])


