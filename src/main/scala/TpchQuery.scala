package main.scala

import java.io._

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
abstract class TpchQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(sc: SparkContext, tpchSchemaProvider: TpchSchemaProvider, tpchConf: TpchConf): DataFrame

}

object TpchQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {

    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format(
        "com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
  }

  def executeQueries(
    sc: SparkContext,
    schemaProvider: TpchSchemaProvider,
    queryNum: Int,
    tpchConf: TpchConf): ListBuffer[(String, Float)] = {

    // if set write results to hdfs, if null write to stdout
    // val OUTPUT_DIR: String = "/tpch"
    /*val OUTPUT_DIR: String = "file://" + new File(".").getAbsolutePath() + "/dbgen/output"*/
    val OUTPUT_DIR = tpchConf.getString("all.hdfs") + tpchConf.getString("all.output-dir")

    val results = new ListBuffer[(String, Float)]

    var fromNum = 1
    var toNum = 22
    if (queryNum != 0) {
      fromNum = queryNum
      toNum = queryNum
    }

    for (queryNo <- fromNum to toNum) {
      val t0 = System.nanoTime()
      val query = Class.forName(f"main.scala.Q$queryNo%02d").newInstance.asInstanceOf[TpchQuery]
      outputDF(query.execute(sc, schemaProvider, tpchConf), OUTPUT_DIR, query.getName())

      val t1 = System.nanoTime()
      val elapsed = (t1 - t0) / 1000000000.0f // second
      results += new Tuple2(query.getName(), elapsed)
    }

    results
  }


  def main(args: Array[String]): Unit = {

    val tpchConf = new TpchConf()

    val queryNum = tpchConf.getInt("all.query-num")

    val sparkConf = new SparkConf().setAppName(f"Spark TPCH Query-$queryNum")
    val sc = new SparkContext(sparkConf)

        // read files from local FS
    /*val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"*/
    val INPUT_DIR = tpchConf.getString("all.hdfs") + tpchConf.getString("all.input-dir")

    val schemaProvider = new TpchSchemaProvider(sc, INPUT_DIR)

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(sc, schemaProvider, queryNum, tpchConf)

    val outFile = new File("TIMES.txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, value) => bw.write(f"$key%s\t$value%1.8f\n")
    }

    bw.close()
  }
}
