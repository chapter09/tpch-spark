package main.scala

import java.io.{File, FileInputStream, InputStream}
import scala.collection.mutable

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

/**
  * Created by hao on 2017-04-02.
  */
class TpchConf() {

  private val conf = loadConf()

  private def getCurrentDirectory = new java.io.File(".").getCanonicalPath

  /**
    *  load the configuration file
    */
  private def loadConf(): Config = {
    val confPath = new File(getCurrentDirectory, "/conf/application.conf")
    ConfigFactory.parseFile(confPath)
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

}
