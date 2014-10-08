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

class TezJobExecutionContextTests extends StarkTest {
  
  @Test
  def validateInitialization(){
    val tec = new TezJobExecutionContext
    assertTrue(ReflectionUtils.getFieldValue(tec, "tezConfig").isInstanceOf[TezConfiguration])
    assertTrue(ReflectionUtils.getFieldValue(tec, "fs").isInstanceOf[FileSystem])
    assertTrue(ReflectionUtils.getFieldValue(tec, "cachedRDDs").isInstanceOf[Set[_]])
  }
  
  @Test
  def validatePersist(){
    
    val tec = new TezJobExecutionContext
    val sc = mock(classOf[SparkContext])
    val rdd = new TezRDD("foo", sc, classOf[TextInputFormat], 
        classOf[Text], classOf[IntWritable], 
        (ReflectionUtils.getFieldValue(tec, "tezConfig").asInstanceOf[TezConfiguration]))
    
//    tec.persist(sc, rdd, StorageLevel.NONE)
  }

}