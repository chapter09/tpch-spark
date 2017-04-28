package main.scala

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}
import collection.JavaConverters._


/**
  * Created by hao on 2017-04-02.
  */
class TpchConf(path: String = null) {

  private val conf = loadConf()

  private def getCurrentDirectory = new java.io.File(".").getCanonicalPath

  /**
    *  load the configuration file
    */
  private def loadConf(): Config = {
    if (path == null || path.isEmpty) {
      ConfigFactory.parseFile(new File(getCurrentDirectory, "/conf/application.conf"))
    } else {
      ConfigFactory.parseFile(new File(path))
    }
  }

  def getString(key: String): String = {
    conf.getString(key)
  }

  def getBoolean(key: String): Boolean = {
    conf.getBoolean(key)
  }

  def getInt(key: String): Int = {
    conf.getInt(key)
  }

  def getAnyRef(key: String): Any = {
    conf.getAnyRef(key)
  }

  def getStringList(key: String): List[String] = {
    conf.getStringList(key).asScala.toList
  }

}
