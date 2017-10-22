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

/*
REPL:
import utils._

val rspark = SparkUtils.rspark
import rspark.implicits._
val empJson = "/Users/esumitra/workspaces/scala/bigdata-processing/assignment5/src/main/resources/employees.json"
val readme = "/Users/esumitra/workspaces/scala/bigdata-processing/assignment5/src/main/resources/README.md"
val df = rspark.read.json(empJson)
// mongo DB
import com.mongodb.spark._
import org.bson.Document
import scala.util.{Try}
val mspark = SparkUtils.mspark
import mspark.implicits._
val lineRDD = mspark.read.textFile(readme).filter(line => line.contains("a")).rdd
lineRDD.flatMap(line2Document(_))
def line2Document(l: String) = Try(new Document("line", l)).toOption
 */
