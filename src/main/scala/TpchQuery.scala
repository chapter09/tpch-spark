package main.scala

import java.io._

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.SizeEstimator
import org.apache.spark.storage.StorageLevel
//import org.apache.spark.sql.Row
//import org.apache.spark.rdd.RDD
//import org.apache.spark.rdd

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

  val getName: String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(sc: SparkContext, tpchSchemaProvider: TpchSchemaProvider, tpchConf: TpchConf): DataFrame

}

object TpchQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String): DataFrame = {

    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format(
        "com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
      df
  }

//  def calcRDDSize(rdd: RDD[String]): Long = {
//    rdd.map(_.getBytes("UTF-8").length.toLong)
//      .reduce(_+_) //add the sizes together
//  }

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
      val df = outputDF(query.execute(sc, schemaProvider, tpchConf), OUTPUT_DIR, query.getName)

      val t1 = System.nanoTime()
      val elapsed = (t1 - t0) / 1000000000.0f // second

      results += Tuple2(sc.appName, elapsed)
    }

    results
  }


  def main(args: Array[String]): Unit = {

    val tpchConf = new TpchConf(if(args == null || args.isEmpty) null else args(0))

    val queryNum = tpchConf.getInt("all.query-num")
    val appSuffix = tpchConf.getString("all.app-suffix")

    val sparkConf = new SparkConf()
      .setAppName(f"Q$queryNum-$appSuffix")

    val sc = new SparkContext(sparkConf)

    // read files from local FS
    /*val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"*/
    val INPUT_DIR = tpchConf.getString("all.hdfs") + tpchConf.getString("all.input-dir")

    val schemaProvider = if (queryNum == 23) null else new TpchSchemaProvider(sc, tpchConf, INPUT_DIR)

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(sc, schemaProvider, queryNum, tpchConf)

    val outFile = new File("TIMES.txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, size, value) => bw.write(f"$key%s\t$size\t$value%1.8f\n")
    }

    bw.close()
  }
}
