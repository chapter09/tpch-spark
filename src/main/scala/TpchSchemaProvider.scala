package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

// TPC-H table schemas
case class Customer(
  c_custkey: Int,
  c_name: String,
  c_address: String,
  c_nationkey: Int,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String)

case class Lineitem(
  l_orderkey: Int,
  l_partkey: Int,
  l_suppkey: Int,
  l_linenumber: Int,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

case class Nation(
  n_nationkey: Int,
  n_name: String,
  n_regionkey: Int,
  n_comment: String)

case class Order(
  o_orderkey: Int,
  o_custkey: Int,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Int,
  o_comment: String)

case class Part(
  p_partkey: Int,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Int,
  p_container: String,
  p_retailprice: Double,
  p_comment: String)

case class Partsupp(
  ps_partkey: Int,
  ps_suppkey: Int,
  ps_availqty: Int,
  ps_supplycost: Double,
  ps_comment: String)

case class Region(
  r_regionkey: Int,
  r_name: String,
  r_comment: String)

case class Supplier(
  s_suppkey: Int,
  s_name: String,
  s_address: String,
  s_nationkey: Int,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String)

class TpchSchemaProvider(sc: SparkContext, tpchConf: TpchConf, inputDir: String) {

  // this is used to implicitly convert an RDD to a DataFrame.
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  val dataScale = tpchConf.getString("all.data-scale")

  val dfMap = Map(
    "customer" -> sc.textFile(inputDir + s"/customer-$dataScale.tbl*").map(_.split('|')).map(p =>
      Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt,
        p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF(),

    "lineitem" -> sc.textFile(inputDir + s"/lineitem-$dataScale.tbl*").map(_.split('|')).map(p =>
      Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt,
        p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble,
        p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF(),

    "nation" -> sc.textFile(inputDir + s"/nation-$dataScale.tbl*").map(_.split('|')).map(p =>
      Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF(),

    "region" -> sc.textFile(inputDir + s"/region-$dataScale.tbl*").map(_.split('|')).map(p =>
      Region(p(0).trim.toInt, p(1).trim, p(2).trim)).toDF(),

    "order" -> sc.textFile(inputDir + s"/orders-$dataScale.tbl*").map(_.split('|')).map(p =>
      Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble,
        p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF(),

    "part" -> sc.textFile(inputDir + s"/part-$dataScale.tbl*").map(_.split('|')).map(p =>
      Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim,
        p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF(),

    "partsupp" -> sc.textFile(inputDir + s"/partsupp-$dataScale.tbl*").map(_.split('|')).map(p =>
      Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble,
        p(4).trim)).toDF(),

    "supplier" -> sc.textFile(inputDir + s"/supplier-$dataScale.tbl*").map(_.split('|')).map(p =>
      Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt,
        p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF())

  // for implicits
  val customer: DataFrame = dfMap("customer")
  val lineitem: DataFrame = dfMap("lineitem")
  val nation: DataFrame = dfMap("nation")
  val region: DataFrame = dfMap("region")
  val order: DataFrame = dfMap("order")
  val part: DataFrame = dfMap("part")
  val partsupp: DataFrame = dfMap("partsupp")
  val supplier: DataFrame = dfMap("supplier")

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
}
