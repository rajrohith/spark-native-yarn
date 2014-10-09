package org.apache.spark.tez.test.utils

import org.apache.spark.tez.adapter.SparkToTezAdapter
import javassist.ClassPool
import org.mockito.Mockito
import org.apache.tez.dag.api.client.DAGClient
import sun.misc.Unsafe

class StarkTest {
  SparkToTezAdapter.adapt
  TezClientTestInstrumenter.instrumentForTests
}
/**
 *
 */
object TezClientTestInstrumenter {
  private var instrumented = false;

  /**
   *
   */
  def instrumentForTests() {
    if (!instrumented) {
      val unsafe = this.createUnsafe
      val cl = Thread.currentThread().getContextClassLoader()

      val pool = ClassPool.getDefault();

      val tezClientClass = pool.get("org.apache.tez.client.TezClient")
      val tezClientMethods = tezClientClass.getDeclaredMethods
      for (targetMethod <- tezClientMethods) {
        if (targetMethod.getName() == "waitTillReady") {
          targetMethod.setBody("return;")
        } else if (targetMethod.getName() == "submitDAG") {
          //      targetMethod.setBody("return (org.apache.tez.dag.api.client.DAGClient)org.mockito.Mockito.mock(org.apache.tez.dag.api.client.DAGClient.class);")
          targetMethod.setBody("try {\n" +
            "org.apache.tez.dag.api.client.DAGClient client = (org.apache.tez.dag.api.client.DAGClient)org.mockito.Mockito.mock(org.apache.tez.dag.api.client.DAGClient.class);\n" +
            "org.apache.tez.dag.api.client.DAGStatus dagStatus = (org.apache.tez.dag.api.client.DAGStatus)org.mockito.Mockito.mock(org.apache.tez.dag.api.client.DAGStatus.class);\n" +
            "org.mockito.Mockito.when(dagStatus.getState()).thenReturn(org.apache.tez.dag.api.client.DAGStatus.State.SUCCEEDED);\n" +
            "org.mockito.Mockito.when(client.waitForCompletionWithStatusUpdates(org.mockito.Mockito.anySet())).thenReturn(dagStatus);\n" +
            "return client;\n" +
            "} catch (Exception e) {\n" +
            "throw new IllegalStateException(e);  }")
        }
      }
      val tezClientClassBytes = tezClientClass.toBytecode()
      unsafe.defineClass(null, tezClientClassBytes, 0, tezClientClassBytes.length, cl, cl.getClass.getProtectionDomain())
      instrumented = true
    }
  }

  /**
   *
   */
  private def createUnsafe = {
    val field = classOf[Unsafe].getDeclaredField("theUnsafe");
    field.setAccessible(true);
    field.get(null).asInstanceOf[Unsafe]
  }
}