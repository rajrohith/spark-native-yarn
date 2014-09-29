package dev.demo

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
 * Implementation of MultipleTextOutputFormat which will endure that each 
 * unique key is written to a unique file essentially grouping keys in single file
 * 
 */
class KeyPerPartitionOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any = {
    NullWritable.get()
  }

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    key.asInstanceOf[String]
  }
}