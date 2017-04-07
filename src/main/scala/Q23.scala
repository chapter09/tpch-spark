package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * TPC-H Query X
  * Created by hao <haowang@ece.utoronto.ca> on 2017-04-02.
  *
  */

class Q23 extends TpchQuery {

  def execute(sc: SparkContext,
    schemaProvider: TpchSchemaProvider,
    conf: TpchConf): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val spark = SparkSession
    .builder()
    .appName("TPCH-Q23")
    .getOrCreate()


    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    val tableA = conf.getString("Q23.table-a")
    val tableB = conf.getString("Q23.table-b")

    val sqlDF = spark.sql(s"SELECT * from $Partsupp")
    sqlDF.show()
    sqlDF

//    schemaProvider.lineitem.filter($"l_shipdate" <= "1998-09-02")
//      .groupBy($"l_returnflag", $"l_linestatus")
//      .agg(sum($"l_quantity"), sum($"l_extendedprice"),
//        sum(decrease($"l_extendedprice", $"l_discount")),
//        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
//        avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
//      .sort($"l_returnflag", $"l_linestatus")
  }

}
