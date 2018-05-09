name := "odif_spark_util"

version := "1.0"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.databricks" %% "spark-csv" % "1.4.0",
  "mysql" % "mysql-connector-java" % "6.0.4"
)
        