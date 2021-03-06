name := "Spark-TPCH-queries"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.2" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3" % "provided"
