package org.example

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentialsProviderChain, EnvironmentVariableCredentialsProvider}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object SparkAwsApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    //Loading credentials from ~/.aws/credentials
    val credentials = new AWSCredentialsProviderChain(new
        EnvironmentVariableCredentialsProvider(), new
        ProfileCredentialsProvider())

    //Setting accesskey and secretkey to spark session
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getCredentials.getAWSAccessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getCredentials.getAWSSecretKey)

    val inputPath = args(0)
    val outputPath = args(1)
    val inputDs = spark.read.textFile(inputPath)

    //Approach 1: Using DS approach

    /*    val inputDf = inputDs.filter(x => !x.matches("[a-zA-Z,\\t]*"))
          .map(x => x.split("[\t,]").toList)
          .map(x => if(x.length==2) (x(0).trim.toInt,x(1).trim.toInt) else (x(0).trim.toInt,0))
          .toDF("Key", "Value")*/

    //Approach 2: Reading DataFrame as csv from DataSet...Both approaches are working

    //Ignoring headers and Replacing rows with common delimiter
    val processedDs = inputDs.filter(x => !x.matches("[a-zA-Z,\\t]*"))
      .map(x => x.replaceAll("[\t,]", ","))

    //Loading data as DataFrame and replacing null/empty values with 0
    val inputDf = spark.read.option("inferSchema", "true")
      .csv(processedDs)
      .toDF("Key", "Value")
      .na.fill(0, Array("Value"))
      .na.fill("0", Array("Value"))

    //filtering keys which are having odd number of values
    val outputDf = inputDf
      .groupBy("Key", "Value")
      .agg(count("Value").as("count"))
      .filter("mod(count,2)=1")
      .drop("count")

    //Writing output to given location either local or s3
    outputDf.coalesce(1).write
      .option("header", "true").option("delimiter", "\t")
      .mode("overwrite")
      .csv(outputPath)

  }
}
