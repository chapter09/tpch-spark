package main.scala.TableProvider

import main.scala._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by hao <haowang@ece.utoronto.ca> on 2017-04-02.
  */

abstract class TpchTable(tpchConf: TpchConf) {

  val dataScale: Int = tpchConf.getInt("all.data-scale")
  val inputDir: String = tpchConf.getString("all.input-dir")

  def create(sparkSession: SparkSession): DataFrame

  def destroy(sparkSession: SparkSession): DataFrame

}


class Customer(conf: TpchConf) extends TpchTable(conf)  {

  override def destroy(sparkSession: SparkSession): DataFrame = {
    sparkSession.sql("DROP TABLE IF EXISTS customer")
  }

  override def create(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("CREATE EXTERNAL TABLE IF NOT EXISTS customer " +
      "(c_custkey Int, c_name String, c_address String, c_nationkey Int, " +
      "c_phone String, c_acctbal Double, c_mktsegment String, c_comment String) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\|' " +
      f" LOCATION '$inputDir/customer-$dataScale'")
  }

}


class Lineitem(conf: TpchConf) extends TpchTable(conf) {

  override def destroy(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("DROP TABLE IF EXISTS lineitem")

  }

  override def create(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("CREATE EXTERNAL TABLE IF NOT EXISTS lineitem " +
      "(l_orderkey Int, l_partkey Int, l_suppkey Int, l_linenumber Int, " +
      "l_quantity Double, l_extendedprice Double, l_discount Double, l_tax Double, " +
      "l_returnflag String, l_linestatus String, l_shipdate String, l_commitdate String, " +
      "l_receiptdate String, l_shipinstruct String, l_shipmode String, l_comment String) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\|' " +
      f"STORED AS TEXTFILE LOCATION '$inputDir/lineitem-$dataScale'")
  }

}


class Nation(conf: TpchConf) extends TpchTable(conf) {

  override def destroy(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("DROP TABLE IF EXISTS nation")

  }

  override def create(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("CREATE EXTERNAL TABLE IF NOT EXISTS nation " +
      "(n_nationkey Int, n_name String, n_regionkey Int, n_comment String) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\|' " +
      f"STORED AS TEXTFILE LOCATION '$inputDir/nation-$dataScale'")
  }
}


class Order(conf: TpchConf) extends TpchTable(conf) {

  override def destroy(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("DROP TABLE IF EXISTS order")

  }

  override def create(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("CREATE EXTERNAL TABLE IF NOT EXISTS order " +
      "(o_orderkey Int, o_custkey Int, o_orderstatus String, " +
      "o_totalprice Double, o_orderdate String, o_orderpriority String, " +
      "o_clerk String,  o_shippriority Int, o_comment String) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\|' " +
      f"STORED AS TEXTFILE LOCATION '$inputDir/orders-$dataScale'")
  }
}


class Part(conf: TpchConf) extends TpchTable(conf) {

  override def destroy(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("DROP TABLE IF EXISTS part")

  }

  override def create(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("CREATE EXTERNAL TABLE IF NOT EXISTS part " +
      "(p_partkey Int, p_name String, p_mfgr String, p_brand String, " +
      "p_type String, p_size Int, p_container String, p_retailprice Double, p_comment String) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\|' " +
      f"STORED AS TEXTFILE LOCATION '$inputDir/part-$dataScale'")
  }
}


class Partsupp(conf: TpchConf) extends TpchTable(conf) {

  override def destroy(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("DROP TABLE IF EXISTS partsupp")

  }

  override def create(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("CREATE EXTERNAL TABLE IF NOT EXISTS partsupp " +
      "(ps_partkey Int, ps_suppkey Int, ps_availqty Int, ps_supplycost Double, ps_comment String) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\|' " +
      f"STORED AS TEXTFILE LOCATION '$inputDir/partsupp-$dataScale'")
  }
}


class Region(conf: TpchConf) extends TpchTable(conf) {

  override def destroy(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("DROP TABLE IF EXISTS region")

  }

  override def create(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("CREATE EXTERNAL TABLE IF NOT EXISTS region " +
      "(r_regionkey Int, r_name String, r_comment String) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\|' " +
      f"STORED AS TEXTFILE LOCATION '$inputDir/region-$dataScale'")
  }
}


class Supplier(conf: TpchConf) extends TpchTable(conf) {

  override def destroy(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("DROP TABLE IF EXISTS supplier")

  }

  override def create(sparkSession: SparkSession): DataFrame = {

    sparkSession.sql("CREATE EXTERNAL TABLE IF NOT EXISTS supplier " +
      "(s_suppkey Int, s_name String, s_address String, s_nationkey Int," +
      " s_phone String, s_acctbal Double, s_comment String) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\|' " +
      f"STORED AS TEXTFILE LOCATION '$inputDir/supplier-$dataScale'")
  }
}

