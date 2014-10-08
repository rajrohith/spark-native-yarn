package org.apache.spark.tez.io

import org.junit.Test
import org.mockito.Mockito._
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.tez.dag.api.TezConfiguration
import java.io.FileNotFoundException
import org.junit.Assert._
import org.apache.spark.tez.test.utils.StarkTest
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.tez.SparkUtils
import java.util.Map
import org.apache.tez.runtime.api.LogicalInput
import org.apache.tez.runtime.api.LogicalOutput

class TezRDDTests extends StarkTest {
  
  @Test
  def failInitializationWithNonExistingSource() {
    val sc = mock(classOf[SparkContext])
    try {
      init("foo")
      fail
    } catch {
      case e: FileNotFoundException =>
    }
  }
  
  @Test
  def validateInitializationWithExistingSource() {
    val sc = mock(classOf[SparkContext])
    try {
      init("README.md")
    } catch {
      case e: FileNotFoundException => fail
    }
  }
  
  @Test
  def validateFQPath() {
//    val sc = mock(classOf[SparkContext])
//    val tezRdd = new TezRDD("README.md", sc, classOf[TextInputFormat],
//        classOf[Text], classOf[IntWritable], new TezConfiguration)
//    assertTrue(tezRdd.getPath.isAbsolute())
    
  }
  
  @Test
  def validateCompute() {
    val sc = mock(classOf[SparkContext])
    val tezRdd = new TezRDD("README.md", sc, classOf[TextInputFormat],
        classOf[Text], classOf[IntWritable], new TezConfiguration)
    // to be continued
//    val inMap = new java.util.HashMap[Integer, LocalInput]()
//    val outMap = mock(classOf[Map[Integer, LogicalOutput]])
//    val sm = new TezShuffleManager(inMap, outMap, false);
//    SparkUtils.createSparkEnv(sm)
//    val partition = mock(classOf[Partition])
//    val tc = mock(classOf[TaskContext])
//    tezRdd.compute(partition, tc)
  }
  
  private def init(path:String){
    val sc = mock(classOf[SparkContext])
    new TezRDD(path, sc, classOf[TextInputFormat],
        classOf[Text], classOf[IntWritable], new TezConfiguration)
  }
}