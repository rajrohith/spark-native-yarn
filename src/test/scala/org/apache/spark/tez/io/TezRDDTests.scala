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
import java.io.File
import org.apache.spark.tez.test.utils.TestLogicalInput

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
      init("src/test/scala/org/apache/spark/tez/io/tezRDDTestFile.txt")
    } catch {
      case e: FileNotFoundException => fail
    }
  }

  @Test
  def validateFQPath() {
    val sc = mock(classOf[SparkContext])
    val tezRdd = new TezRDD("src/test/scala/org/apache/spark/tez/io/tezRDDTestFile.txt", sc, classOf[TextInputFormat],
      classOf[Text], classOf[IntWritable], new TezConfiguration)
    assertTrue(tezRdd.getPath.isAbsolute())
  }
  
  @Test
  def validateGetPartitions() = {
    val sc = mock(classOf[SparkContext])
    val tezRdd = new TezRDD("src/test/scala/org/apache/spark/tez/io/tezRDDTestFile.txt", sc, classOf[TextInputFormat],
      classOf[Text], classOf[IntWritable], new TezConfiguration)
    val partitions = tezRdd.getPartitions;
    assertNotNull(partitions)
    assertTrue(partitions.size == 1)
  }
  
  @Test
  def validateToString() = {
    val sc = mock(classOf[SparkContext])
    val tezRdd = new TezRDD("src/test/scala/org/apache/spark/tez/io/tezRDDTestFile.txt", sc, classOf[TextInputFormat],
      classOf[Text], classOf[IntWritable], new TezConfiguration)
    assertEquals("name:src/test/scala/org/apache/spark/tez/io/tezRDDTestFile.txt; " + 
        "path:file:/Users/ozhurakousky/dev/fork/stark/src/test/scala/org/apache/spark/tez/io/tezRDDTestFile.txt", tezRdd.toString)
  }

  @Test
  def validateCompute() {
    val file = new File("src/test/scala/org/apache/spark/tez/io/tezRDDTestFile.txt");
    val sc = mock(classOf[SparkContext])
    val tezRdd = new TezRDD(file.getAbsolutePath(), sc, classOf[TextInputFormat],
      classOf[Text], classOf[IntWritable], new TezConfiguration)

    val inMap = new java.util.HashMap[Integer, LogicalInput]()
    inMap.put(0, new TestLogicalInput(file.toURI()))
    val outMap = mock(classOf[Map[Integer, LogicalOutput]])
    outMap.put(1, mock(classOf[LogicalOutput]))
    val sm = new TezShuffleManager(inMap, outMap, false);
    SparkUtils.createSparkEnv(sm)
    val partition = mock(classOf[Partition])
    val tc = mock(classOf[TaskContext])
    val iterator = tezRdd.compute(partition, tc)
    
    assertNotNull(iterator)
    assertEquals(3, iterator.toList.size)
  }

  private def init(path: String) {
    val sc = mock(classOf[SparkContext])
    new TezRDD(path, sc, classOf[TextInputFormat],
      classOf[Text], classOf[IntWritable], new TezConfiguration)
  }
}