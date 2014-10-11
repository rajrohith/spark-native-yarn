package org.apache.spark.tez.test.utils

import org.apache.spark.tez.adapter.SparkToTezAdapter
import javassist.ClassPool
import org.mockito.Mockito
import org.apache.tez.dag.api.client.DAGClient
import sun.misc.Unsafe

class Instrumentable {
  SparkToTezAdapter.adapt
}
