package test.scala

import main.scala.TableProvider.TpchTable
import main.scala.TpchConf
import scala.reflect.internal.util.ScalaClassLoader

import scala.reflect.runtime.universe._

/**
  * Created by hao on 2017-04-12.
  */
object TestTableProvider {

  def getClassInstance(clsName: String): Any = {
    val mirror = runtimeMirror(getClass.getClassLoader)
    val cls = mirror.classSymbol(Class.forName(clsName))
    val module = cls.companionSymbol.asModule
    mirror.reflectModule(module).instance
  }

  def main(args: Array[String]): Unit = {

    val conf = new TpchConf()

    val tableA = Class.forName("main.scala.TableProvider.Customer")
////      .getConstructor()
////      .newInstance(conf)
////      .asInstanceOf[TpchTable]
//
//
//    val tableB = getClassInstance("main.scala.TableProvider.Customer")

    val x = tableA.getConstructor(conf.getClass).newInstance(conf).asInstanceOf[TpchTable]

    println(x)

  }

}
