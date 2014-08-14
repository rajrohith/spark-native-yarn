package org.apache.spark.tez

import org.apache.spark.rdd.RDD

class VertexResultTask(rdd:RDD[_], func:Function2[_,_,_]) extends VertexTask(rdd) {

}