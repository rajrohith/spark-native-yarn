package com.hortonworks.spark.tez.repl

import scala.collection.mutable.ArrayBuffer
import java.net.URLClassLoader
import java.io.File
import org.apache.spark.repl.SparkILoop
import com.hortonworks.spark.tez.utils.JarUtils
import scala.tools.nsc.interpreter.ILoop
import scala.tools.nsc.Settings

object SparkTezReplConsole extends App {
  val settings = new Settings
  val classPathExclusions = JarUtils.initClasspathExclusions("repl_exclusions")

  val cl = getClass.getClassLoader
  var paths = new ArrayBuffer[String]
  if (cl.isInstanceOf[URLClassLoader]) {
    val urlLoader = cl.asInstanceOf[URLClassLoader]
    for (url <- urlLoader.getURLs) {
      if (url.getProtocol == "file") {
        if (JarUtils.useClassPathEntry(url.getFile(), classPathExclusions)) {
          settings.classpath.append(url.getFile)
        }
      }
    }
  }

  settings.usejavacp.value = true
  settings.deprecation.value = true

  new SampleILoop().process(settings)
}

class SampleILoop extends ILoop {
  override def prompt = "spark-on-tez> "
  var printPrompt = true
  override def resetCommand() {
    this.command("sc.stop")
    super.resetCommand
    setupImports
  }

  addThunk {
    setupImports
  }
  
  def setupImports() {
    intp.beQuietDuring {
//      println()
    
      intp.addImports("org.apache.hadoop.fs.FileSystem")
      intp.addImports("org.apache.hadoop.fs.Path")
      intp.addImports("org.apache.hadoop.yarn.conf.YarnConfiguration")
      intp.addImports("org.apache.spark.SparkContext")
      intp.addImports("org.apache.spark.SparkContext._")
      intp.addImports("org.apache.spark.Tez")
//      val result = intp.interpret("val sc = new SparkContext(\"local\", \"MyApp\")")
//      println(result)
//      doWelcome
      this.command("val sc = new SparkContext(\"local\", \"MyApp\") with Tez")
      echo(doWelcome)
      if (printPrompt){
        printPrompt = false;
        this.in.redrawLine
      }
    }
  }

  override def printWelcome() {
    echo("Starting Spark on Tez interactive console")
//    intp.asInstanceOf[scala.tools.nsc.interpreter.ILoop$ILoopInterpreter]
    
  }
  def doWelcome() = {
    "\n" +
      "                           \\,,,/\n" +
      "                           (o o)\n" +
      "                  -----oOOo-(_)-oOOo-----\n" +
      "  _____                  _                    _______     \n" +
      " / ____|                | |                  |__   __|      \n" +
      "| (___  _ __   __ _ _ __| | __   ___  _ __      | | ___ ____\n" +
      " \\___ \\| '_ \\ / _` | '__| |/ /  / _ \\| '_ \\     | |/ _ \\_  /\n" +
      " ____) | |_) | (_| | |  |   <  | (_) | | | |    | |  __// / \n" +
      "|_____/| .__/ \\__,_|_|  |_|\\_\\  \\___/|_| |_|    |_|\\___/___|\n" +
      "      | |                                                  \n" +
      "      |_|                                                   \n" +
      "The following imports are added for your convinince:\n" + 
      "import org.apache.hadoop.fs.FileSystem\n" + 
      "import org.apache.hadoop.fs.Path\n" +
      "import org.apache.hadoop.yarn.conf.YarnConfiguration\n" + 
      "import org.apache.spark.SparkContext\n" +
      "import org.apache.spark.SparkContext._\n" +
      "import org.apache.spark.Tez"
  }
}