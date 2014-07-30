package org.apache.spark

import java.io.File
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.nio.ByteBuffer
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{ InputFormat => NewInputFormat }
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ResultTask
import org.apache.spark.scheduler.ShuffleMapTask
import org.apache.spark.scheduler.Stage
import org.apache.tez.dag.api.TezConfiguration
import com.hortonworks.spark.tez.DAGBuilder
import com.hortonworks.spark.tez.DAGBuilder.VertexDescriptor
import com.hortonworks.spark.tez.utils.TypeAwareStreams.TypeAwareObjectOutputStream
import org.apache.spark.rdd.ParallelCollectionRDD
import scala.collection.mutable.ArrayBuffer
import org.apache.tez.client.TezClient

trait Tez extends SparkContext {

  var outputPath:String = this.appName + "_out"
  val tezConfiguration = new TezConfiguration(new Configuration)
  val tezClient = new TezClient(this.appName, this.tezConfiguration);
  var dagBuilder:DAGBuilder = null

  var taskCounter = 0;

  val ser = SparkEnv.get.closureSerializer.newInstance()

  val inputMap = new HashMap[String, Tuple3[Class[_], Class[_], Class[_]]]()

  
  /**
   * 
   */
  override def runJob[T, U: ClassTag](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    allowLocal: Boolean,
    resultHandler: (Int, U) => Unit) {
    
	taskCounter = 0;
    dagBuilder = new DAGBuilder(tezClient, this.appName, tezConfiguration, outputPath)
    println("Intercepting Spark Job submission and delegating it to Tez")
    
    val stage = this.createStage(rdd, this.dagScheduler)

    this.prepStages(stage, null, func)

    println("Current DAG: " + dagBuilder)

    dagBuilder.build.execute();
   
    val fs = FileSystem.get(tezConfiguration);

    val key = new LongWritable
    val value = new Text
    val fStatus = fs.listFiles(new Path(outputPath), false)
    val l:ArrayBuffer[Tuple2[_,_]] = if (classOf[Option[_]].isAssignableFrom(evidence$1.runtimeClass)) null else new ArrayBuffer[Tuple2[_,_]]
    var counter = 0
    
    while (fStatus.hasNext()) {
      val status = fStatus.next()
      val file = status.getPath()
      if (file.getName().contains("part")) {
        val fileSplit = new FileSplit(file, 0, status.getLen(), null)
        val ti = new TaskAttemptID
        val ctx = new TaskAttemptContextImpl(new Configuration, ti)
        val reader = new SequenceFileAsBinaryInputFormat.SequenceFileAsBinaryRecordReader
        reader.initialize(fileSplit, ctx)

        while (reader.nextKeyValue()) {
          val kv = reader.getCurrentValue().copyBytes()
          val tuple = ser.deserialize[Tuple2[_, _]](ByteBuffer.wrap(kv))
         
          if (classOf[Option[_]].isAssignableFrom(evidence$1.runtimeClass)){
            resultHandler.apply(counter, tuple._2.asInstanceOf[U])
          }
          else {
            l += tuple
          }
        }
        fs.delete(file)
        if (l != null){
          val s = l.toArray
          resultHandler.apply(counter, s.asInstanceOf[U])
          l.clear
        }
        counter += 1
      }
    }
  }

  override def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
    path: String,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V],
    conf: Configuration = hadoopConfiguration): RDD[(K, V)] = {

    this.inputMap += ((path, (inputFormatClass, keyClass, valueClass)))

    val hadoopRDD = new NewHadoopRDD(
      this,
      inputFormatClass,
      keyClass,
      valueClass,
      null) with HadoopRDDMixin[K, V]
    hadoopRDD.setName(path)

    // Set 'broadcast' variable which broadcasts Hadoop configuration to null.
    // We don't need it during de-serilization and it avoids nasty NPE
    val fields = hadoopRDD.getClass.getSuperclass().getDeclaredFields()
    for (field <- fields if (field.getName().endsWith("confBroadcast"))) {
      field.setAccessible(true)
      field.set(hadoopRDD, null)
    }

    hadoopRDD
  }

  override def textFile(path: String, minPartitions: Int = defaultMinPartitions): RDD[String] = {
    newAPIHadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text]).map(pair => pair._2.toString).setName(path)
  }

  override def hadoopFile[K, V](
    path: String,
    inputFormatClass: Class[_ <: org.apache.hadoop.mapred.InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = {
    throw new UnsupportedOperationException("Old Hadoop API is no longer supported. Please use newAPIHadoopFile(..) or tex/sequenceFile(..) methods")
  }

  private def prepStages[T, U: ClassTag](stage: Stage, dependentStage: Stage, func: (TaskContext, Iterator[T]) => U) {
    if (stage.parents.size > 0) {
      val missing = stage.parents.sortBy(_.id)
      for (parent <- missing) {
        prepStages(parent, stage, func)
      }
    }
    val dependencies = stage.rdd.getNarrowAncestors.sortBy(_.id)
    val firstDependency = dependencies(0)
    val deps = (if (firstDependency.name == null) (for (parent <- stage.parents) yield parent.id).asJavaCollection else firstDependency.name)
    val vd = new VertexDescriptor(stage.id, taskCounter, deps)
    if (deps.isInstanceOf[String]) {
      val inputs = this.inputMap.get(deps.asInstanceOf[String]).get
      vd.setInputFormatClass(inputs._1)
      vd.setKey(inputs._2)
      vd.setValue(inputs._3)
    } else {
      vd.setNumPartitions(stage.numPartitions)
    }
    this.dagBuilder.addVertex(vd)

    val tmpDir = new File(System.getProperty("java.io.tmpdir") + "/" + this.appName)
    tmpDir.mkdirs()
    val file = new File(tmpDir, "SparkTask_" + taskCounter + ".ser")
    if (file.exists()) {
      val deleted = file.delete()
      if (!deleted) {
        throw new IllegalStateException("Failed to delete file: " + file.getAbsolutePath())
      }
    }
    taskCounter += 1

    val vertextTask =
//      if (classOf[AbstractFileClassLoader].isAssignableFrom(Thread.currentThread().getContextClassLoader().getClass)) {
        if (stage.isShuffleMap) {
          new TezShuffleTask(stage.id, stage.rdd, stage.shuffleDep.get, 0, null)
        } else {
          new TezResultTask(stage.id, stage.rdd.asInstanceOf[RDD[T]], func, 0, Nil, 0)
        }
//      } else {
//        if (stage.isShuffleMap) {
//          new ShuffleMapTask(stage.id, stage.rdd, stage.shuffleDep.get, 0, null)
//        } else {
//          new ResultTask(stage.id, stage.rdd.asInstanceOf[RDD[T]], func, 0, Nil, 0)
//        }
//      }

    val os = new TypeAwareObjectOutputStream(new FileOutputStream(file))

    ser.serializeStream(os)
    os.writeObject(vertextTask);
    os.close();
    println("Serialized " + file.getAbsolutePath() + " - " + file.length())
  }

  private def createStage(rdd: RDD[_], dagScheduler: AnyRef): Stage = {
    val method = dagScheduler.getClass.getDeclaredMethod("newStage", classOf[RDD[_]], classOf[Int], classOf[Option[ShuffleDependency[_, _, _]]], classOf[Int], classOf[Option[String]])
    method.setAccessible(true)
    val stage = method.invoke(dagScheduler, rdd, new Integer(1), None, new Integer(0), None).asInstanceOf[Stage]
    stage
  }
}

/**
 *
 */
trait HadoopRDDMixin[K, V] extends RDD[(K, V)] {
  /*
   * if not overriden you get this:
   * Caused by: java.io.IOException: No input paths specified in job
	at org.apache.hadoop.mapred.FileInputFormat.listStatus(FileInputFormat.java:201)
	at org.apache.hadoop.mapred.FileInputFormat.getSplits(FileInputFormat.java:304)
	at org.apache.spark.rdd.HadoopRDD.getPartitions(HadoopRDD.scala:172)
   */
  override def getPartitions: Array[Partition] = {
    Array(new Partition {
      override def index: Int = 0
    })
  }

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
//    println("Computing in HadoopRDD")
    val iterator = SparkEnv.get.shuffleManager.getReader(null, 0, 0, null).read.asInstanceOf[Iterator[(K, V)]]
    new InterruptibleIterator(new TaskContext(0, 1, 1, true), iterator)
  }
}