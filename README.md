
## Spark and NoSQL Databases
This project provides samples for connecting Spark and NoSQL databases  in Scala. Two NoSQL database connection samples are provided.

 - **MongoDB**: CP from CAP theorem, document store
 - **DynamoDB**: AP from CAP theorem, key-value store

The project requires Java 8, Scala 2.11.8 and sbt 0.13.16 environment to run.

### Running the MongoDB Sample
 -  Compile and generate the  "fat" jar from the project
  `sbt assembly`

 - Run the spark job on locally or in the cloud
    e.g., `$SPARK_HOME/bin/spark-submit --class "example.LetterCountMongo" --master local[4] /fullpath/assignment5.jar /inputpath/README.md mongodb://127.0.0.1/resources.wordCount2`

 - The output of the spark word count job will be written to the wordCount2 collection specified in the command line arguments above

### Running the DynamoDB Sample
 -  Compile and generate the  "fat" jar from the project
  `sbt assembly`

 - Copy the dynamo connector jar to the spark master node
e.g., 
`scp -i ~/.ssh/bigdata.pem emr-dynamodb-hadoop-4.2.0.jar hadoop@ec2-3x-xxx-xx-xx.compute-1.amazonaws.com:/home/hadoop/emr-dynamodb-hadoop-4.2.0.jar
 `
 - Run the job on AWS EMR
Create a cluster on AWS EMR with Spark application and add a new step for a Spark job with the following:

- Spark submit options 
  `--class "example.LetterCountDynamo" --jars /home/hadoop/emr-dynamodb-hadoop-4.2.0.jar`
- arguments
`s3://cscie88-bigdata/spark-lettercount/README.md wordcount`

 -  The output of the spark job will be in the Dynamo DB table wordcount

### License
Copyright 2017, Edward Sumitra

Licensed under the Apache License, Version 2.0.

