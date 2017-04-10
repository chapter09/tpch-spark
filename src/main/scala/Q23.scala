package main.scala

import org.apache.spark.SparkContext
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
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
//    import schemaProvider._

    val warehouseLocation = conf.getString("all.input-dir")

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS customer "
    + "(c_custkey Int, c_name String, c_address String, c_nationkey Int, "
    + "c_phone String, c_acctbal Double, c_mktsegment String, c_comment String)")

    sql(f"LOAD DATA INPATH '/tpch/customer.tbl' INTO TABLE customer")

    sql("SELECT * FROM customer").toDF()


    //val tableA = conf.getString("Q23.table-a")
    //val tableB = conf.getString("Q23.table-b")

    //val sqlDF = spark.sql(s"SELECT * from $Partsupp")
    //sqlDF.show()
    //sqlDF

  }

}
