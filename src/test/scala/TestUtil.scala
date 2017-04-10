package test.scala

import main.scala.TpchConf
import java.io.File
import com.typesafe.config.ConfigFactory

/**
  * Created by hao on 2017-04-02.
  */
object TestUtil {

  def main(args: Array[String]): Unit = {
//    val configPath = System.getProperty("")
//    println(configPath)
    val tpchConf = new TpchConf()

    println(tpchConf.getString("all.input-dir"))
    println(tpchConf.getString("Q23.table-b"))

  }
}
