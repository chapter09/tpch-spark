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

    val warehouseLocation = "/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.worker.cores", "1")
      .config("spark.worker.instances", "4")
      .config("spark.worker.memory", "2G")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val tableList = conf.getStringList("Q23.table-list")
    tableList.foreach((table: String) =>
      Class.forName("main.scala.TableProvider." + table.capitalize)
        .getConstructor(conf.getClass)
        .newInstance(conf)
        .asInstanceOf[TpchTable]
        .create(spark)
    )

    val df = sql(conf.getString("Q23.query"))

    tableList.foreach((table: String) =>
      Class.forName("main.scala.TableProvider." + table.capitalize)
        .getConstructor(conf.getClass)
        .newInstance(conf)
        .asInstanceOf[TpchTable]
        .destroy(spark)
    )

    df
  }
}
