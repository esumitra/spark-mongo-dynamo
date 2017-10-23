/**
  * utilities for using using spark
  */

package utils

import org.apache.spark.sql.SparkSession

object SparkUtils {
  lazy val rspark =
    SparkSession
      .builder()
      .appName("Spark REPL")
      .master("local[*]")
      .getOrCreate()

  lazy val spark =
    SparkSession
      .builder()
      .appName("Lab Spark Session")
      .getOrCreate()

  lazy val mspark =
    session(SparkConfiguration(
      "Mongo Spark",
      Some("local[*]"),
      Some(Map(
        "spark.mongodb.output.uri" -> "mongodb://127.0.0.1/resources.letterCount"))))

  /**
    * returns Spark Session for input configuration
    */
  def session(conf: SparkConfiguration): SparkSession = {
    val builder =
      SparkSession
        .builder()
        .appName(conf.name)

    // set master if supplied
    conf.masterConf.map(builder.master(_))

    // set config props if any
    conf.props.map { props =>
      for ((k,v) <- props) {
        builder.config(k, v)
      }
    }

    builder.getOrCreate
  }

  
}

