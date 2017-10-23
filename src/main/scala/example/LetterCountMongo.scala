/**
  * letter count written to Mongo DB
  * using MongoDB connector from https://docs.mongodb.com/spark-connector/master/
  */

package example

import utils._
import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import java.util.HashMap
import org.bson.Document
import scala.util.{Try}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._

/**
  * reads the input file, filters lines with "a" and writes to mongo DB
  * to run local:
  * 1. create a "fat" jar using 'sbt assembly' command first
  * 2. $SPARK_HOME/bin/spark-submit --class "example.LetterCountMongo" --master local[4] /Users/esumitra/workspaces/scala/bigdata-processing/assignment5/target/scala-2.11/assignment5.jar /Users/esumitra/workspaces/scala/bigdata-processing/assignment5/src/main/resources/README.md mongodb://127.0.0.1/resources.letterCount2
  */
object LetterCountMongo {

  def main(args: Array[String]): Unit = {
    val inputFile:String = args(0)
    val outputMongoURL = args(1)
    val conf =
      SparkConfiguration(
        "mongodb-session",
        None,
        Some(Map("spark.mongodb.output.uri" -> outputMongoURL)))
    val ses = SparkUtils.session(conf)
    writeCounts(ses, args(0))
    ses.stop();
  }

  def readInputFile(spark: SparkSession, inFilePath: String): RDD[String] =
    spark.read.textFile(inFilePath).cache().rdd

  /**
    * reads input file, calculates word count and writes to Mongo DB
    */
  def writeCounts(spark: SparkSession,  inFilePath: String): Unit = {
    val input = readInputFile(spark, inFilePath)
    val counts = line2Counts(input)
      .map((t:Tuple2[String, Long]) => wc2Document(t))
      .filter(!_.isEmpty)
      .map(_.get)
    counts
      .saveToMongoDB()
  }

  def line2Counts(lines: RDD[String]): RDD[Tuple2[String, Long]] =
    lines
      .flatMap(line => line.split(" "))
      .map(word => (word, 1L))
      .reduceByKey(_ + _)

    /**
    * convert word count to Mongo JSON document
    * lots of java classes here since connector supports java classes primarily
    */
  def wc2Document(wc: Tuple2[String, Long]): Option[Document] =
    Try {
      val wcMap = new HashMap[String, Object]()
      wcMap.put("word", wc._1)
      wcMap.put("count", (wc._2: java.lang.Long))
      new Document(wcMap)
    }.toOption

}
