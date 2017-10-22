/**
  * Spark letter count example
  * with Dynamo DB
  * using connector from https://search.maven.org/#artifactdetails%7Ccom.amazon.emr%7Cemr-dynamodb-hadoop%7C4.2.0%7Cjar
  */

package example

import utils._
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.rdd.RDD
import java.util.HashMap
import org.apache.hadoop.io.{Text}
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import com.amazonaws.services.dynamodbv2.model.AttributeValue

/**
  * to run on AWS EMR, start a cluster and add a step with Spark in cluster mode
  * spark submit options: 
  * --class "example.LetterCountDynamo"
  * --jars /home/hadoop/emr-dynamodb-hadoop-4.2.0.jar
  * application: s3://cscie88-bigdata/spark-lettercount/assignment5.jar
  * args: 
  *  s3://cscie88-bigdata/spark-lettercount/README.md
  *  wordcount
  */

case class WordCount(word: String, count: Long)

object WordCount {
  def apply(t: Tuple2[String, Long]): WordCount = WordCount(t._1, t._2)
}

object LetterCountDynamo {

  def main(args: Array[String]): Unit = {
    val ses = SparkUtils.spark
    writeCounts(ses, args(0), args(1))
    ses.stop();
  }

  def line2Counts(lines: RDD[String]): RDD[WordCount] =
    lines
      .flatMap(line => line.split(" "))
      .map(word => (word, 1L))
      .reduceByKey(_ + _)
      .map(WordCount(_))

  def WordCount2DynamoMap(wc: WordCount): Tuple2[Text, DynamoDBItemWritable] = {
    val ddbMap = new HashMap[String, AttributeValue]()
    val wordValue = new AttributeValue()
    wordValue.setS(wc.word)
    ddbMap.put("word", wordValue)
    val countValue = new AttributeValue()
    countValue.setN(wc.count.toString())
    ddbMap.put("count", countValue)
    val item = new DynamoDBItemWritable()
    item.setItem(ddbMap)
    (new Text(""), item)
  }

  def writeToDynamo(rdd: RDD[Tuple2[Text, DynamoDBItemWritable]]) = {
    val jobConf2 = new JobConf(rdd.context.hadoopConfiguration)
    jobConf2.set("dynamodb.servicename", "dynamodb");
    jobConf2.set("dynamodb.endpoint", "dynamodb.us-east-1.amazonaws.com");
    jobConf2.set("dynamodb.regionid", "us-east-1");
    jobConf2.set("dynamodb.output.tableName", "wordcount")
    jobConf2.set("dynamodb.throughput.write.percent", "0.5")
    jobConf2.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
    jobConf2.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    rdd
      .saveAsHadoopDataset(jobConf2)
  }

  def readInputFile(spark: SparkSession, inFilePath: String): RDD[String] =
    spark.read.textFile(inFilePath).cache().rdd

  def writeCounts(spark: SparkSession,  inFilePath: String, outTable:String): Unit = {
    val input = readInputFile(spark, inFilePath)
    val counts = line2Counts(input).map(WordCount2DynamoMap(_))
    writeToDynamo(counts)
  }
}

/*
REPL:
import example._
import utils._
val rspark = SparkUtils.rspark
import rspark.implicits._
val readme = "/Users/esumitra/workspaces/scala/bigdata-processing/assignment5/src/main/resources/README.md"
val ld = LetterCountDynamo.readInputFile(rspark, readme)
val wc = LetterCountDynamo.line2Counts(ld)

// aws commands
scp -i ~/.ssh/esumitra-bigdata.pem emr-dynamodb-hadoop-4.2.0.jar hadoop@ec2-34-228-55-17.compute-1.amazonaws.com:/home/hadoop/emr-dynamodb-hadoop-4.2.0.jar
ssh -i ~/.ssh/esumitra-bigdata.pem hadoop@ec2-34-228-55-17.compute-1.amazonaws.com
 */
