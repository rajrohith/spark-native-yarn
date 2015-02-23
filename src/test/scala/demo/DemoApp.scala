package demo

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.tez.TezJobExecutionContext
import org.apache.spark.tez.TezConstants

object DemoApp {
  
  val value = "Rhett's cancer treatment wiped out antibodies the child developed after being " + 
		  " vaccinated when he was a baby. His immune system is still too weak to get new shots, so he " + 
		  " depends on herd immunity being surrounded by others who are vaccinated -- to stay healthy.";

  def main(args:Array[String]) {
   
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName("SparkOnTez-Demo")
    val sc = new SparkContext(sparkConf)
    
    val source = sc.parallelize(List(value), 2);
    
    val job = source.flatMap(_.split(" ")).map{x => 
      println("hello")
      (x, 1)}.reduceByKey(_ + _);
   
    println("Result: " + job.collect.toList)
    sc.stop
  }
  
  
  def buildSparkConf(masterUrl:String = "execution-context:" + classOf[TezJobExecutionContext].getName): SparkConf = {
    System.setProperty(TezConstants.GENERATE_JAR, "true")
    System.setProperty(TezConstants.UPDATE_CLASSPATH, "true")
    
    val sparkConf = new SparkConf
    sparkConf.set("spark.ui.enabled", "false")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    
    sparkConf.setMaster(masterUrl)
    sparkConf
  }
}