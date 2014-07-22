package com.hortonworks.tez.spark

object WordCountWithTez extends App {

//  prepTestFile
//
//  val sc = new SparkContext("local", "SparkOnTez") with Tez
//  
//  val source = sc.textFile("sample.txt")
// 
//  source
//  	.flatMap(line =>for (x <- line.split(" ")) yield new Tuple2[String, Int](x, 1))
//  	.reduceByKey(_ + _)
//  	.collect
//
//  private def prepTestFile() {
//    try {
//      val fs = FileSystem.get(new YarnConfiguration);
//      val testFile = new Path("sample.txt");
//      val out = fs.create(testFile, true);
//      var i = 0;
//      for (i <- 1 to 10000) {
//        out.write("hello world ".getBytes());
//      }
//      out.close();
//    } catch {
//      case e: Exception => e.printStackTrace()
//    }
//  }
}