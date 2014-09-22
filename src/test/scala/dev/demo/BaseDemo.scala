package dev.demo

import java.io.File
import java.net.URL
import java.net.URLClassLoader

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.tez.TezConstants
import org.apache.tez.dag.api.TezConfiguration

trait BaseDemo {

  def prepare(jobName: String, inputFiles: Array[String]) {
    val cl = ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader]
    val m = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL]);
    m.setAccessible(true);
    val file = new File("src/test/resources/mini/").toURI().toURL()
    m.invoke(cl, file)

    if (jobName != null && inputFiles != null) {
      System.setProperty(TezConstants.GENERATE_JAR, "true");
      val configuration = new TezConfiguration

      val fs = FileSystem.get(configuration);
      fs.delete(new Path(jobName + "_out"))
      inputFiles.foreach(x => fs.copyFromLocalFile(new Path(x), new Path(x)))
    }
  }
}