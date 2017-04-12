package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import TableProvider._

/**
  * TPC-H Query X
  * Created by hao <haowang@ece.utoronto.ca> on 2017-04-02.
  *
  */

class Q23 extends TpchQuery {

  def execute(sc: SparkContext,
    schemaProvider: TpchSchemaProvider,
    conf: TpchConf): DataFrame = {

    val warehouseLocation = conf.getString("all.hdfs") + conf.getString("all.input-dir")

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val tableA = Class.forName("main.scala.TableProvider." +
      conf.getString("Q23.table-a").capitalize)
      .getConstructor(conf.getClass)
      .newInstance(conf)
      .asInstanceOf[TpchTable]
      .create(spark)

    val tableB = Class.forName("main.scala.TableProvider." +
      conf.getString("Q23.table-b").capitalize)
      .getConstructor(conf.getClass)
      .newInstance(conf)
      .asInstanceOf[TpchTable]
      .create(spark)

    sql(f"SELECT * FROM ${conf.getString("Q23.table-a")} WHERE c_custkey=12")
    sql(f"SELECT * FROM ${conf.getString("Q23.table-b")} WHERE l_orderkey=12")

  }

}
