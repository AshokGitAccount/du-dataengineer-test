# DU Data Engineer Test

Spark application to read CSV, TSV file from local or AWS S3, and apply business logic. 


### Prerequisites

For running program in local install, ensure intelliJ or scala softaware is installed.

Import project in Intellij and build it suing below two sbt commands

sbt clean
sbt package

Once packaging is done, copy du-dataengineer-test_2.11-0.1.jar file from from target/scala-s.11/ folder to a server where spark is available.

This program can be submitted directly to AWS EMR as well if spark application is selected while creating EMR cluster.

copy input files to specific folder in local or s3 bucket. this is folder path as input while running application.


### Running Spark Application

For running on AWS or any windows machine :

Ensure that spark software is installed in the machine and spark-submit command is working.

This program will take two arguments

1) Input file path.
2) Ouput directory path to store processed data.

spark-submit --master local[*] --class org.example.SparkAwsApp --packages com.amazonaws:aws-java-sdk
:1.11.698 path\to\jar\du-dataengineer-test_2.11-0.1.jar input\path\to\files\input output\path\output_data