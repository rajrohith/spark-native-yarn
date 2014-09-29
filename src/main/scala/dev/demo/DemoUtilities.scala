package dev.demo

import java.io.{InputStreamReader, BufferedReader, File}
import java.net.URL
import java.net.URLClassLoader

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.tez.TezConstants
import org.apache.tez.dag.api.TezConfiguration

object DemoUtilities {

  def prepareJob(inputFiles: Array[String]) : String = {
    val jobName = this.getClass().getName() + "-" + System.currentTimeMillis()
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
    jobName
  }

  /**
   *
   */
  def printSampleResults(outputPath: String) {
    val conf = new TezConfiguration
    val fs = FileSystem.get(conf);
    val iter = fs.listFiles(new Path(outputPath), false);
    var counter = 0;
    var run = true
    while (iter.hasNext() && run) {
      val status = iter.next();
      if (status.isFile()) {
        if (!status.getPath().toString().endsWith("_SUCCESS")) {
          println("Results from " + status.getPath() + " - " + fs.getLength(status.getPath()))

          val reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())))
          var line: String = null
          var r = true
          while (r && counter < 20) {
            line = reader.readLine()
            if (line == null) {
              r = false
            } else {
              println("\t" + line)
              counter += 1
            }
          }
        }
      }
    }
  }
}