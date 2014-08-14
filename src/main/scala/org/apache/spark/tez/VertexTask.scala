package org.apache.spark.tez

import org.apache.spark.rdd.RDD

class VertexTask(val rdd:RDD[_]) extends Serializable {

}