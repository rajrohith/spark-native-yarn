package org.apache.spark.tez

import org.junit.Test
import scala.reflect.runtime.universe._
import java.lang.reflect.Field
import org.junit.Assert._
import org.apache.tez.dag.api.TezConfiguration
import org.apache.hadoop.fs.FileSystem
import scala.collection.mutable.Set
import org.apache.hadoop.fs.Path
import org.mockito.Mockito._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.storage.StorageLevel
import org.apache.spark.tez.io.TezRDD
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.spark.tez.test.utils.ReflectionUtils
import org.apache.spark.tez.test.utils.StarkTest
import org.mockito.Mockito
import org.mockito.internal.matchers.Any
import org.mockito.Mock
import org.apache.spark.tez.test.utils.MockRddUtils
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import java.io.File

class TezJobExecutionContextTests extends StarkTest {
  
  @Test
  def validateInitialization(){
    val tec = new TezJobExecutionContext
    assertTrue(ReflectionUtils.getFieldValue(tec, "tezConfig").isInstanceOf[TezConfiguration])
    assertTrue(ReflectionUtils.getFieldValue(tec, "fs").isInstanceOf[FileSystem])
    assertTrue(ReflectionUtils.getFieldValue(tec, "cachedRDDs").isInstanceOf[Set[_]])
  }
  
  @Test
  def validateInitialPersist(){
    val tec = new TezJobExecutionContext
    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
    val sc = new SparkContext(masterUrl, "validatePersist")
    val rdd = new TezRDD("src/test/scala/org/apache/spark/tez/sample.txt", sc, classOf[TextInputFormat], 
        classOf[Text], classOf[IntWritable], 
        (ReflectionUtils.getFieldValue(tec, "tezConfig").asInstanceOf[TezConfiguration]))

    val persistedRddName = "validatePersist_cache_" + rdd.id
    val file = new File(persistedRddName)
    file.createNewFile()
    file.deleteOnExit()

    val persistedRdd = tec.persist(sc, rdd, StorageLevel.NONE)
    val set = ReflectionUtils.getFieldValue(tec, "cachedRDDs").asInstanceOf[Set[Path]]
    assertEquals(1, set.size)
    assertEquals(new File(persistedRddName).toURI().toString(), set.iterator.next.toUri().toString())
    assertNotNull(persistedRdd)
    assertTrue(new File(persistedRddName).exists())
  }
  
  @Test
  def validateSubsequentPersist(){
    val tec = new TezJobExecutionContext
    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
    val sc = new SparkContext(masterUrl, "validatePersist")
    val rdd = new TezRDD("src/test/scala/org/apache/spark/tez/sample.txt", sc, classOf[TextInputFormat], 
        classOf[Text], classOf[IntWritable], 
        (ReflectionUtils.getFieldValue(tec, "tezConfig").asInstanceOf[TezConfiguration]))

    val persistedRddName = "validatePersist_cache_" + rdd.id
    val file = new File(persistedRddName)
    file.createNewFile()
    file.deleteOnExit()

//    val sRdd = MockRddUtils.stubSaveAsTexFile(rdd, persistedRddName)
    val persistedRdd = tec.persist(sc, rdd, StorageLevel.NONE)
    tec.persist(sc, persistedRdd, StorageLevel.NONE)
    assertNotNull(persistedRdd)
    assertTrue(new File(persistedRddName).exists())
  }

}