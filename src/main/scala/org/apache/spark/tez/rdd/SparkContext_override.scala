/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.tez.rdd

import java.io.File
import java.io.Serializable
import java.net.URI
import java.util.UUID
import java.util.UUID.randomUUID
import java.util.concurrent.atomic.AtomicInteger
import scala.Option.option2Iterable
import scala.collection.Map
import scala.collection.Set
import scala.collection.mutable.HashMap
import scala.language.implicitConversions
import scala.reflect.ClassTag
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.hadoop.mapreduce.{Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.Accumulable
import org.apache.spark.AccumulableParam
import org.apache.spark.Accumulator
import org.apache.spark.AccumulatorParam
import org.apache.spark.DAGExecutor
import org.apache.spark.GrowableAccumulableParam
import org.apache.spark.SerializableWritable
import org.apache.spark.SimpleFutureAction
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.SparkFiles
import org.apache.spark.TaskContext
import org.apache.spark.WritableConverter
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.Experimental
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.input.WholeTextFileInputFormat
import org.apache.spark.partial.ApproximateEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.rdd.CheckpointRDD
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.rdd.ParallelCollectionRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.rdd.WholeTextFileRDD
import org.apache.spark.scheduler.EventLoggingListener
import org.apache.spark.scheduler.Schedulable
import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.storage.RDDInfo
import org.apache.spark.storage.StorageStatus
import org.apache.spark.storage.StorageUtils
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.CallSite
import org.apache.spark.util.ClosureCleaner
import org.apache.spark.util.TimeStampedWeakValueHashMap
import org.apache.spark.util.Utils
import scala.collection.generic.Growable
import org.apache.hadoop.mapred.SequenceFileInputFormat


/**
 * Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
 * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
 *
 * @param config a Spark Config object describing the application configuration. Any settings in
 *   this config overrides the default configs as well as system properties.
 */

class SparkContext_override(config: SparkConf)  {

  // This is used only by YARN for now, but should be relevant to other cluster types (Mesos,
  // etc) too. This is typically generated from InputFormatInfo.computePreferredLocations. It
  // contains a map from hostname to a list of input format splits on the host.
  private var preferredNodeLocationData: Map[String, Set[SplitInfo]] = Map()

  /**
   * Create a SparkContext that loads settings from system properties (for instance, when
   * launching with ./bin/spark-submit).
   */
  def this() = this(new SparkConf())

  /**
   * :: DeveloperApi ::
   * Alternative constructor for setting preferred locations where Spark will create executors.
   *
   * @param preferredNodeLocationData used in YARN mode to select nodes to launch containers on.
   * Can be generated using [[org.apache.spark.scheduler.InputFormatInfo.computePreferredLocations]]
   * from a list of input files or InputFormats for the application.
   */
  @DeveloperApi
  def this(config: SparkConf, preferredNodeLocationData: Map[String, Set[SplitInfo]]) = {
    this(config)
    this.preferredNodeLocationData = preferredNodeLocationData
  }

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param conf a [[org.apache.spark.SparkConf]] object specifying other Spark parameters
   */
  def this(master: String, appName: String, conf: SparkConf) =
    this(SparkContext.updatedConf(conf, master, appName))

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   * @param environment Environment variables to set on worker nodes.
   */
  def this(
      master: String,
      appName: String,
      sparkHome: String = null,
      jars: Seq[String] = Nil,
      environment: Map[String, String] = Map(),
      preferredNodeLocationData: Map[String, Set[SplitInfo]] = Map()) =
  {
    this(SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment))
    this.preferredNodeLocationData = preferredNodeLocationData
  }

  // NOTE: The below constructors could be consolidated using default arguments. Due to
  // Scala bug SI-8479, however, this causes the compile step to fail when generating docs.
  // Until we have a good workaround for that bug the constructors remain broken out.

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   */
  private def this(master: String, appName: String) =
    this(master, appName, null, Nil, Map(), Map())

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.
   */
  private def this(master: String, appName: String, sparkHome: String) =
    this(master, appName, sparkHome, Nil, Map(), Map())

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   */
  private def this(master: String, appName: String, sparkHome: String, jars: Seq[String]) =
    this(master, appName, sparkHome, jars, Map(), Map())

  private val conf = config.clone()
  conf.validateSettings()

  /**
   * Return a copy of this SparkContext's configuration. The configuration ''cannot'' be
   * changed at runtime.
   */
  def getConf: SparkConf = conf.clone()

  if (!conf.contains("spark.master")) {
    throw new SparkException("A master URL must be set in your configuration")
  }
  if (!conf.contains("spark.app.name")) {
    throw new SparkException("An application name must be set in your configuration")
  }

  if (conf.getBoolean("spark.logConf", false)) {
    //logInfo("Spark configuration:\n" + conf.toDebugString)
  }

  // Set Spark driver host and port system properties
  conf.setIfMissing("spark.driver.host", Utils.localHostName())
  conf.setIfMissing("spark.driver.port", "0")

  val jars: Seq[String] =
    conf.getOption("spark.jars").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten

  val files: Seq[String] =
    conf.getOption("spark.files").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten

  val master = conf.get("spark.master")
  val dagExecutor:DAGExecutor = {
    val EXTERNAL = "(external:)(.*)".r
    master match {
      case EXTERNAL(external, dagExecutorClassName) =>
        Class.forName(dagExecutorClassName).newInstance.asInstanceOf[DAGExecutor]
      case _ => null
    }
  }
  val appName = conf.get("spark.app.name")

  // Generate the random name for a temp folder in Tachyon
  // Add a timestamp as the suffix here to make it more safe
  val tachyonFolderName = "spark-" + randomUUID.toString()
  conf.set("spark.tachyonStore.folderName", tachyonFolderName)

  val isLocal = (master == "local" || master.startsWith("local["))

  if (master == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")

  // An asynchronous listener bus for Spark events
//  private val listenerBus = new LiveListenerBus

  // Create the Spark execution environment (cache, map output tracker, etc)
  private val env = SparkEnv.create(
    conf,
    "<driver>",
    conf.get("spark.driver.host"),
    conf.get("spark.driver.port").toInt,
    isDriver = true,
    isLocal = isLocal,
    listenerBus = null)
  SparkEnv.set(env)

  // Used to store a URL for each static file/jar together with the file's local timestamp
  private val addedFiles = HashMap[String, Long]()
  private val addedJars = HashMap[String, Long]()

  // Keeps track of all persisted RDDs
  private val persistentRdds = new TimeStampedWeakValueHashMap[Int, RDD[_]]
//  private val metadataCleaner =
//    new MetadataCleaner(MetadataCleanerType.SPARK_CONTEXT, this.cleanup, conf)

  // Initialize the Spark UI, registering all associated listeners
  private val ui = new SparkUI(this.asInstanceOf[SparkContext])
  ui.bind()

  /** A default Hadoop Configuration for the Hadoop code (e.g. file systems) that we reuse. */
  val hadoopConfiguration: Configuration = new YarnConfiguration

  // Optionally log Spark events
  private val eventLogger: Option[EventLoggingListener] = {
    if (conf.getBoolean("spark.eventLog.enabled", false)) {
      val logger = new EventLoggingListener(appName, conf, hadoopConfiguration)
      logger.start()
      //listenerBus.addListener(logger)
      Some(logger)
    } else None
  }

  // At this point, all relevant SparkListeners have been registered, so begin releasing events
  //listenerBus.start()

  val startTime = System.currentTimeMillis()

  // Add each JAR given through the constructor
  if (jars != null) {
    jars.foreach(addJar)
  }

  if (files != null) {
    files.foreach(addFile)
  }

//  private def warnSparkMem(value: String): String = {
//    logWarning("Using SPARK_MEM to set amount of memory to use per executor process is " +
//      "deprecated, please use spark.executor.memory instead.")
//    value
//  }

//  private val executorMemory = conf.getOption("spark.executor.memory")
//    .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
//    .orElse(Option(System.getenv("SPARK_MEM")).map(warnSparkMem))
//    .map(Utils.memoryStringToMb)
//    .getOrElse(512)

  // Environment variables to pass to our executors.
  private val executorEnvs = HashMap[String, String]()

  // Convert java options to env vars as a work around
  // since we can't set env vars directly in sbt.
  for { (envKey, propKey) <- Seq(("SPARK_TESTING", "spark.testing"))
    value <- Option(System.getenv(envKey)).orElse(Option(System.getProperty(propKey)))} {
    executorEnvs(envKey) = value
  }
//  Option(System.getenv("SPARK_PREPEND_CLASSES")).foreach { v =>
//    executorEnvs("SPARK_PREPEND_CLASSES") = v
//  }
  // The Mesos scheduler backend relies on this environment variable to set executor memory.
  // TODO: Set this only in the Mesos scheduler.
//  executorEnvs("SPARK_EXECUTOR_MEMORY") = executorMemory + "m"
//  executorEnvs ++= conf.getExecutorEnv

  // Set SPARK_USER for user who is running SparkContext.
  val sparkUser = Option {
    Option(System.getenv("SPARK_USER")).getOrElse(System.getProperty("user.name"))
  }.getOrElse {
    SparkContext.SPARK_UNKNOWN_USER
  }
  executorEnvs("SPARK_USER") = sparkUser

  // Create and start the scheduler
//  private var taskScheduler = SparkContext.createTaskScheduler(this, master)
//  private val heartbeatReceiver = if (taskScheduler != null) env.actorSystem.actorOf(
//    Props(new HeartbeatReceiver(taskScheduler)), "HeartbeatReceiver") else null
//  @volatile private var dagScheduler: DAGScheduler = _
//  try {
//    if (dagExecutor == null){
//      dagScheduler = new DAGScheduler(this)
//    }
//  } catch {
//    case e: Exception => throw
//      new SparkException("DAGScheduler cannot be initialized due to %s".format(e.getMessage))
//  }
//
//  // start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's
//  // constructor
//  if (taskScheduler != null){ // not delegating to external executor
//    taskScheduler.start()
//  }
//
//  private val cleaner: Option[ContextCleaner] = {
//    if (conf.getBoolean("spark.cleaner.referenceTracking", true)) {
//      Some(new ContextCleaner(this))
//    } else {
//      None
//    }
//  }
//  cleaner.foreach(_.start())
//
//  postEnvironmentUpdate()
//  postApplicationStart()

  var checkpointDir: String = ""

  // Thread Local variable that can be used by users to pass information down the stack
//  private val localProperties = new InheritableThreadLocal[Properties] {
//    override protected def childValue(parent: Properties): Properties = new Properties(parent)
//  }

//  private def getLocalProperties: Properties = localProperties.get()
//
//  private def setLocalProperties(props: Properties) {
//    localProperties.set(props)
//  }
//
//  @deprecated("Properties no longer need to be explicitly initialized.", "1.0.0")
//  def initLocalProperties() {
//    localProperties.set(new Properties())
//  }

  /**
   * Set a local property that affects jobs submitted from this thread, such as the
   * Spark fair scheduler pool.
   */
  def setLocalProperty(key: String, value: String) {
//    if (localProperties.get() == null) {
//      localProperties.set(new Properties())
//    }
//    if (value == null) {
//      localProperties.get.remove(key)
//    } else {
//      localProperties.get.setProperty(key, value)
//    }
  }

  /**
   * Get a local property set in this thread, or null if it is missing. See
   * [[org.apache.spark.SparkContext.setLocalProperty]].
   */
  def getLocalProperty(key: String): String = null
//    Option(localProperties.get).map(_.getProperty(key)).getOrElse(null)

  /** Set a human readable description of the current job. */
  @deprecated("use setJobGroup", "0.8.1")
  def setJobDescription(value: String) {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, value)
  }

  /**
   * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
   * different value or cleared.
   *
   * Often, a unit of execution in an application consists of multiple Spark actions or jobs.
   * Application programmers can use this method to group all those jobs together and give a
   * group description. Once set, the Spark web UI will associate such jobs with this group.
   *
   * The application can also use [[org.apache.spark.SparkContext.cancelJobGroup]] to cancel all
   * running jobs in this group. For example,
   * {{{
   * // In the main thread:
   * sc.setJobGroup("some_job_to_cancel", "some job description")
   * sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
   *
   * // In a separate thread:
   * sc.cancelJobGroup("some_job_to_cancel")
   * }}}
   *
   * If interruptOnCancel is set to true for the job group, then job cancellation will result
   * in Thread.interrupt() being called on the job's executor threads. This is useful to help ensure
   * that the tasks are actually stopped in a timely manner, but is off by default due to HDFS-1208,
   * where HDFS may respond to Thread.interrupt() by marking nodes as dead.
   */
  def setJobGroup(groupId: String, description: String, interruptOnCancel: Boolean = false) {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, description)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, groupId)
    // Note: Specifying interruptOnCancel in setJobGroup (rather than cancelJobGroup) avoids
    // changing several public APIs and allows Spark cancellations outside of the cancelJobGroup
    // APIs to also take advantage of this property (e.g., internal job failures or canceling from
    // JobProgressTab UI) on a per-job basis.
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, interruptOnCancel.toString)
  }

  /** Clear the current thread's job group ID and its description. */
  def clearJobGroup() {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, null)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, null)
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, null)
  }

//  // Post init
//  if (taskScheduler != null){
//    taskScheduler.postStartHook()
//  }
//  
//
//  private val dagSchedulerSource = new DAGSchedulerSource(this.dagScheduler, this)
//  private val blockManagerSource = new BlockManagerSource(SparkEnv.get.blockManager, this)

//  private def initDriverMetrics() {
////    SparkEnv.get.metricsSystem.registerSource(dagSchedulerSource)
////    SparkEnv.get.metricsSystem.registerSource(blockManagerSource)
//  }

//  initDriverMetrics()

  // Methods for creating RDDs

  /** Distribute a local Scala collection to form an RDD.
   *
   * @note Parallelize acts lazily. If `seq` is a mutable collection and is
   * altered after the call to parallelize and before the first action on the
   * RDD, the resultant RDD will reflect the modified collection. Pass a copy of
   * the argument to avoid this.
   */
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    new ParallelCollectionRDD[T](this.asInstanceOf[SparkContext], seq, numSlices, Map[Int, Seq[String]]())
  }

  /** Distribute a local Scala collection to form an RDD.
   *
   * This method is identical to `parallelize`.
   */
  def makeRDD[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    parallelize(seq, numSlices)
  }

  /** Distribute a local Scala collection to form an RDD, with one or more
    * location preferences (hostnames of Spark nodes) for each object.
    * Create a new partition for each collection item. */
  def makeRDD[T: ClassTag](seq: Seq[(T, Seq[String])]): RDD[T] = {
    val indexToPrefs = seq.zipWithIndex.map(t => (t._2, t._1._2)).toMap
    new ParallelCollectionRDD[T](this.asInstanceOf[SparkContext], seq.map(_._1), seq.size, indexToPrefs)
  }

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   */
  def textFile(path: String, minPartitions: Int = defaultMinPartitions): RDD[String] = {
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }

  /**
   * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI. Each file is read as a single record and returned in a
   * key-value pair, where the key is the path of each file, the value is the content of each file.
   *
   * <p> For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do `val rdd = sparkContext.wholeTextFile("hdfs://a-hdfs-path")`,
   *
   * <p> then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred, large file is also allowable, but may cause bad performance.
   *
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
   */
  def wholeTextFiles(path: String, minPartitions: Int = defaultMinPartitions):
  RDD[(String, String)] = {
    val job = new NewHadoopJob(hadoopConfiguration)
    NewFileInputFormat.addInputPath(job, new Path(path))
    val updateConf = job.getConfiguration
    new WholeTextFileRDD(
      this.asInstanceOf[SparkContext],
      classOf[WholeTextFileInputFormat],
      classOf[String],
      classOf[String],
      updateConf,
      minPartitions).setName(path)
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf given its InputFormat and other
   * necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable),
   * using the older MapReduce API (`org.apache.hadoop.mapred`).
   *
   * @param conf JobConf for setting up the dataset
   * @param inputFormatClass Class of the InputFormat
   * @param keyClass Class of the keys
   * @param valueClass Class of the values
   * @param minPartitions Minimum number of Hadoop Splits to generate.
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def hadoopRDD[K, V](
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions
      ): RDD[(K, V)] = {
    // Add necessary security credentials to the JobConf before broadcasting it.
    SparkHadoopUtil.get.addCredentials(conf)
    new HadoopRDD(this.asInstanceOf[SparkContext], conf, inputFormatClass, keyClass, valueClass, minPartitions)
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat
    *
    * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    * record, directly caching the returned RDD will create many references to the same object.
    * If you plan to directly cache Hadoop writable objects, you should first copy them using
    * a `map` function.
    * */
  def hadoopFile[K, V](
      path: String,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions
      ): RDD[(K, V)] = {
    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = broadcast(new SerializableWritable(hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    new HadoopRDD(
      this.asInstanceOf[SparkContext],
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
  }

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * can just write, for example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path, minPartitions)
   * }}}
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]]
      (path: String, minPartitions: Int)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = {
    hadoopFile(path,
      fm.runtimeClass.asInstanceOf[Class[F]],
      km.runtimeClass.asInstanceOf[Class[K]],
      vm.runtimeClass.asInstanceOf[Class[V]],
      minPartitions)
  }

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * can just write, for example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
   * }}}
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] =
    hadoopFile[K, V, F](path, defaultMinPartitions)

  /** Get an RDD for a Hadoop file with an arbitrary new API InputFormat. */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]]
      (path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = {
    newAPIHadoopFile(
      path,
      fm.runtimeClass.asInstanceOf[Class[F]],
      km.runtimeClass.asInstanceOf[Class[K]],
      vm.runtimeClass.asInstanceOf[Class[V]])
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
      path: String,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V],
      conf: Configuration = hadoopConfiguration): RDD[(K, V)] = {
    val job = new NewHadoopJob(conf)
    NewFileInputFormat.addInputPath(job, new Path(path))
    val updatedConf = job.getConfiguration
    new NewHadoopRDD(this.asInstanceOf[SparkContext], fClass, kClass, vClass, updatedConf).setName(path)
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      conf: Configuration = hadoopConfiguration,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V]): RDD[(K, V)] = {
    new NewHadoopRDD(this.asInstanceOf[SparkContext], fClass, kClass, vClass, conf)
  }

  /** Get an RDD for a Hadoop SequenceFile with given key and value types.
    *
    * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    * record, directly caching the returned RDD will create many references to the same object.
    * If you plan to directly cache Hadoop writable objects, you should first copy them using
    * a `map` function.
    */
  def sequenceFile[K, V](path: String,
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int
      ): RDD[(K, V)] = {
    val inputFormatClass = classOf[SequenceFileInputFormat[K, V]]
    hadoopFile(path, inputFormatClass, keyClass, valueClass, minPartitions)
  }

  /** Get an RDD for a Hadoop SequenceFile with given key and value types.
    *
    * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    * record, directly caching the returned RDD will create many references to the same object.
    * If you plan to directly cache Hadoop writable objects, you should first copy them using
    * a `map` function.
    * */
  def sequenceFile[K, V](path: String, keyClass: Class[K], valueClass: Class[V]
      ): RDD[(K, V)] =
    sequenceFile(path, keyClass, valueClass, defaultMinPartitions)

  /**
   * Version of sequenceFile() for types implicitly convertible to Writables through a
   * WritableConverter. For example, to access a SequenceFile where the keys are Text and the
   * values are IntWritable, you could simply write
   * {{{
   * sparkContext.sequenceFile[String, Int](path, ...)
   * }}}
   *
   * WritableConverters are provided in a somewhat strange way (by an implicit function) to support
   * both subclasses of Writable and types for which we define a converter (e.g. Int to
   * IntWritable). The most natural thing would've been to have implicit objects for the
   * converters, but then we couldn't have an object for every subclass of Writable (you can't
   * have a parameterized singleton object). We use functions instead to create a new converter
   * for the appropriate type. In addition, we pass the converter a ClassTag of its type to
   * allow it to figure out the Writable class to use in the subclass case.
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
   def sequenceFile[K, V]
       (path: String, minPartitions: Int = defaultMinPartitions)
       (implicit km: ClassTag[K], vm: ClassTag[V],
        kcf: () => WritableConverter[K], vcf: () => WritableConverter[V])
      : RDD[(K, V)] = {
    val kc = kcf()
    val vc = vcf()
    val format = classOf[SequenceFileInputFormat[Writable, Writable]]
    val writables = hadoopFile(path, format,
        kc.writableClass(km).asInstanceOf[Class[Writable]],
        vc.writableClass(vm).asInstanceOf[Class[Writable]], minPartitions)
    writables.map { case (k, v) => (kc.convert(k), vc.convert(v)) }
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * BytesWritable values that contain a serialized partition. This is still an experimental
   * storage format and may not be supported exactly as is in future Spark releases. It will also
   * be pretty slow if you use the default serializer (Java serialization),
   * though the nice thing about it is that there's very little effort required to save arbitrary
   * objects.
   */
  def objectFile[T: ClassTag](
      path: String,
      minPartitions: Int = defaultMinPartitions
      ): RDD[T] = {
    sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
      .flatMap(x => Utils.deserialize[Array[T]](x._2.getBytes, Utils.getContextOrSparkClassLoader))
  }

  protected def checkpointFile[T: ClassTag](
      path: String
    ): RDD[T] = {
    new CheckpointRDD[T](this.asInstanceOf[SparkContext], path)
  }

  /** Build the union of a list of RDDs. */
  def union[T: ClassTag](rdds: Seq[RDD[T]]): RDD[T] = new UnionRDD(this.asInstanceOf[SparkContext], rdds)

  /** Build the union of a list of RDDs passed as variable-length arguments. */
  def union[T: ClassTag](first: RDD[T], rest: RDD[T]*): RDD[T] =
    new UnionRDD(this.asInstanceOf[SparkContext], Seq(first) ++ rest)

  /** Get an RDD that has no partitions or elements. */
  def emptyRDD[T: ClassTag] = new EmptyRDD[T](this.asInstanceOf[SparkContext])

  // Methods for creating shared variables

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, which tasks can "add"
   * values to using the `+=` method. Only the driver can access the accumulator's `value`.
   */
  def accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]) =
    new Accumulator(initialValue, param)

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, with a name for display
   * in the Spark UI. Tasks can "add" values to the accumulator using the `+=` method. Only the
   * driver can access the accumulator's `value`.
   */
  def accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T]) = {
    new Accumulator(initialValue, param, Some(name))
  }

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable, to which tasks can add values
   * with `+=`. Only the driver can access the accumuable's `value`.
   * @tparam T accumulator type
   * @tparam R type that can be added to the accumulator
   */
  def accumulable[T, R](initialValue: T)(implicit param: AccumulableParam[T, R]) =
    new Accumulable(initialValue, param)

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable, with a name for display in the
   * Spark UI. Tasks can add values to the accumuable using the `+=` operator. Only the driver can
   * access the accumuable's `value`.
   * @tparam T accumulator type
   * @tparam R type that can be added to the accumulator
   */
  def accumulable[T, R](initialValue: T, name: String)(implicit param: AccumulableParam[T, R]) =
    new Accumulable(initialValue, param, Some(name))

  /**
   * Create an accumulator from a "mutable collection" type.
   *
   * Growable and TraversableOnce are the standard APIs that guarantee += and ++=, implemented by
   * standard mutable collections. So you can use this with mutable Map, Set, etc.
   */
  def accumulableCollection[R <% Growable[T] with TraversableOnce[T] with Serializable: ClassTag, T]
      (initialValue: R): Accumulable[R, T] = {
    val param = new GrowableAccumulableParam[R,T]
    new Accumulable(initialValue, param)
  }

  /**
   * Broadcast a read-only variable to the cluster, returning a
   * [[org.apache.spark.broadcast.Broadcast]] object for reading it in distributed functions.
   * The variable will be sent to each cluster only once.
   */
  def broadcast[T: ClassTag](value: T): Broadcast[T] = {
    val bc = env.broadcastManager.newBroadcast[T](value, isLocal)
//    cleaner.foreach(_.registerBroadcastForCleanup(bc))
    bc
  }

  /**
   * Add a file to be downloaded with this Spark job on every node.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
   * use `SparkFiles.get(path)` to find its download location.
   */
  def addFile(path: String) {
    val uri = new URI(path)
    val key = uri.getScheme match {
      case null | "file" => env.httpFileServer.addFile(new File(uri.getPath))
      case "local"       => "file:" + uri.getPath
      case _             => path
    }
    addedFiles(key) = System.currentTimeMillis

    // Fetch the file locally in case a job is executed using DAGScheduler.runLocally().
    Utils.fetchFile(path, new File(SparkFiles.getRootDirectory()), conf, env.securityManager)

   // logInfo("Added file " + path + " at " + key + " with timestamp " + addedFiles(key))
//    postEnvironmentUpdate()
  }

  /**
   * :: DeveloperApi ::
   * Register a listener to receive up-calls from events that happen during execution.
   */
  @DeveloperApi
  def addSparkListener(listener: SparkListener) {
    //listenerBus.addListener(listener)
  }

  /** The version of Spark on which this application is running. */
  def version = SparkContext.SPARK_VERSION

  /**
   * Return a map from the slave to the max memory available for caching and the remaining
   * memory available for caching.
   */
  def getExecutorMemoryStatus: Map[String, (Long, Long)] = {
    env.blockManager.master.getMemoryStatus.map { case(blockManagerId, mem) =>
      (blockManagerId.host + ":" + blockManagerId.port, mem)
    }
  }

  /**
   * :: DeveloperApi ::
   * Return information about what RDDs are cached, if they are in mem or on disk, how much space
   * they take, etc.
   */
  @DeveloperApi
  def getRDDStorageInfo: Array[RDDInfo] = {
    val rddInfos = persistentRdds.values.map(RDDInfo.fromRdd).toArray
    StorageUtils.updateRddInfo(rddInfos, getExecutorStorageStatus)
    rddInfos.filter(_.isCached)
  }

  /**
   * Returns an immutable map of RDDs that have marked themselves as persistent via cache() call.
   * Note that this does not necessarily mean the caching or computation was successful.
   */
  def getPersistentRDDs: Map[Int, RDD[_]] = {
//    persistentRdds.toMap
    null
  }

  /**
   * :: DeveloperApi ::
   * Return information about blocks stored in all of the slaves
   */
  @DeveloperApi
  def getExecutorStorageStatus: Array[StorageStatus] = {
    env.blockManager.master.getStorageStatus
  }

  /**
   * :: DeveloperApi ::
   * Return pools for fair scheduler
   */
  @DeveloperApi
  def getAllPools: Seq[Schedulable] = {
    // TODO(xiajunluan): We should take nested pools into account
//    if (taskScheduler != null){
//      taskScheduler.rootPool.schedulableQueue.toSeq
//    } else {
//      Nil
//    }
    Nil
  }

  /**
   * :: DeveloperApi ::
   * Return the pool associated with the given name, if one exists
   */
  @DeveloperApi
  def getPoolForName(pool: String): Option[Schedulable] = {
//    if (taskScheduler != null){
//      Option(taskScheduler.rootPool.schedulableNameToSchedulable.get(pool))
//    } else {
//      None
//    }
    None
  }

  /**
   * Return current scheduling mode
   */
  def getSchedulingMode: SchedulingMode.SchedulingMode = {
//    if (taskScheduler != null){
//      taskScheduler.schedulingMode
//    }
//    else {
//      SchedulingMode.NONE
//    }
    SchedulingMode.NONE
  }

  /**
   * Clear the job's list of files added by `addFile` so that they do not get downloaded to
   * any new nodes.
   */
  @deprecated("adding files no longer creates local copies that need to be deleted", "1.0.0")
  def clearFiles() {
//    addedFiles.clear()
  }

  /**
   * Gets the locality information associated with the partition in a particular rdd
   * @param rdd of interest
   * @param partition to be looked up for locality
   * @return list of preferred locations for the partition
   */
  private  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    //dagScheduler.getPreferredLocs(rdd, partition)
    Nil
  }

  /**
   * Register an RDD to be persisted in memory and/or disk storage
   */
  private def persistRDD(rdd: RDD[_]) {
    persistentRdds(rdd.id) = rdd
  }

  /**
   * Unpersist an RDD from memory and/or disk storage
   */
  private def unpersistRDD(rddId: Int, blocking: Boolean = true) {
    env.blockManager.master.removeRdd(rddId, blocking)
    persistentRdds.remove(rddId)
    //listenerBus.post(SparkListenerUnpersistRDD(rddId))
  }

  /**
   * Adds a JAR dependency for all tasks to be executed on this SparkContext in the future.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), an HTTP, HTTPS or FTP URI, or local:/path for a file on every worker node.
   */
  def addJar(path: String) {
//    if (path == null) {
//     // logWarning("null specified as parameter to addJar")
//    } else {
//      var key = ""
//      if (path.contains("\\")) {
//        // For local paths with backslashes on Windows, URI throws an exception
//        key = env.httpFileServer.addJar(new File(path))
//      } else {
//        val uri = new URI(path)
//        key = uri.getScheme match {
//          // A JAR file which exists only on the driver node
//          case null | "file" =>
//            // yarn-standalone is deprecated, but still supported
//            if (SparkHadoopUtil.get.isYarnMode() &&
//                (master == "yarn-standalone" || master == "yarn-cluster")) {
//              // In order for this to work in yarn-cluster mode the user must specify the
//              // --addJars option to the client to upload the file into the distributed cache
//              // of the AM to make it show up in the current working directory.
//              val fileName = new Path(uri.getPath).getName()
//              try {
//                env.httpFileServer.addJar(new File(fileName))
//              } catch {
//                case e: Exception =>
//                  // For now just log an error but allow to go through so spark examples work.
//                  // The spark examples don't really need the jar distributed since its also
//                  // the app jar.
//                //  logError("Error adding jar (" + e + "), was the --addJars option used?")
//                  null
//              }
//            } else {
//              env.httpFileServer.addJar(new File(uri.getPath))
//            }
//          // A JAR file which exists locally on every worker node
//          case "local" =>
//            "file:" + uri.getPath
//          case _ =>
//            path
//        }
//      }
//      if (key != null) {
//        addedJars(key) = System.currentTimeMillis
//        //logInfo("Added JAR " + path + " at " + key + " with timestamp " + addedJars(key))
//      }
//    }
//    postEnvironmentUpdate()
  }

  /**
   * Clear the job's list of JARs added by `addJar` so that they do not get downloaded to
   * any new nodes.
   */
  @deprecated("adding jars no longer creates local copies that need to be deleted", "1.0.0")
  def clearJars() {
//    addedJars.clear()
  }

  /** Shut down the SparkContext. */
  def stop() {
//    postApplicationEnd()
//    ui.stop()
//    // Do this only if not stopped already - best case effort.
//    // prevent NPE if stopped more than once.
//    val dagSchedulerCopy = dagScheduler
//    dagScheduler = null
//    if (dagSchedulerCopy != null) {
//      env.metricsSystem.report()
//      metadataCleaner.cancel()
//      env.actorSystem.stop(heartbeatReceiver)
//      cleaner.foreach(_.stop())
//      dagSchedulerCopy.stop()
//      taskScheduler = null
//      // TODO: Cache.stop()?
//      env.stop()
//      SparkEnv.set(null)
//      listenerBus.stop()
//      eventLogger.foreach(_.stop())
//      logInfo("Successfully stopped SparkContext")
//    } else {
//      logInfo("SparkContext already stopped")
//    }
  }


  /**
   * Get Spark's home location from either a value set through the constructor,
   * or the spark.home Java property, or the SPARK_HOME environment variable
   * (in that order of preference). If neither of these is set, return None.
   */
  private def getSparkHome(): Option[String] = {
//    conf.getOption("spark.home").orElse(Option(System.getenv("SPARK_HOME")))
    None
  }

  /**
   * Support function for API backtraces.
   */
  def setCallSite(site: String) {
//    setLocalProperty("externalCallSite", site)
  }

  /**
   * Support function for API backtraces.
   */
  def clearCallSite() {
//    setLocalProperty("externalCallSite", null)
  }

  /**
   * Capture the current user callsite and return a formatted version for printing. If the user
   * has overridden the call site, this will return the user's version.
   */
  private def getCallSite(): CallSite = {
    Option(getLocalProperty("externalCallSite")) match {
      case Some(callSite) => CallSite(callSite, longForm = "")
      case None => Utils.getCallSite
    }
  }

  /**
   * Run a function on a given set of partitions in an RDD and pass the results to the given
   * handler function. This is the main entry point for all actions in Spark. The allowLocal
   * flag specifies whether the scheduler can run the computation on the driver rather than
   * shipping it out to the cluster, for short actions like first().
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit) {
    
//    if (this.dagExecutor == null){
//      runInSpark(rdd, func, partitions, allowLocal, resultHandler)
//    }
//    else {
      runExternal(rdd, func, partitions, allowLocal, resultHandler)
//    }
  }
  
//  private def runInSpark[T, U: ClassTag](
//      rdd: RDD[T],
//      func: (TaskContext, Iterator[T]) => U,
//      partitions: Seq[Int],
//      allowLocal: Boolean,
//      resultHandler: (Int, U) => Unit) {
//    
////    if (dagScheduler == null) {
////      throw new SparkException("SparkContext has been shutdown")
////    }
////    val callSite = getCallSite
////    val cleanedFunc = clean(func)
////    logInfo("Starting job: " + callSite.shortForm)
////    val start = System.nanoTime
////    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, allowLocal,
////      resultHandler, localProperties.get)
////    logInfo(
////      "Job finished: " + callSite.shortForm + ", took " + (System.nanoTime - start) / 1e9 + " s")
////    rdd.doCheckpoint()
//  }
  
  private def runExternal[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit):Unit = {
//    this.dagExecutor.execute(rdd, func, partitions, allowLocal, resultHandler)
  }

  /**
   * Run a function on a given set of partitions in an RDD and return the results as an array. The
   * allowLocal flag specifies whether the scheduler can run the computation on the driver rather
   * than shipping it out to the cluster, for short actions like first().
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean
      ): Array[U] = {
    val results = new Array[U](partitions.size)
    runJob[T, U](rdd, func, partitions, allowLocal, (index, res) => results(index) = res)
    results
  }

  /**
   * Run a job on a given set of partitions of an RDD, but take a function of type
   * `Iterator[T] => U` instead of `(TaskContext, Iterator[T]) => U`.
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int],
      allowLocal: Boolean
      ): Array[U] = {
    runJob(rdd, (context: TaskContext, iter: Iterator[T]) => func(iter), partitions, allowLocal)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.size, false)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.size, false)
  }

  /**
   * Run a job on all partitions in an RDD and pass the results to a handler function.
   */
  def runJob[T, U: ClassTag](
    rdd: RDD[T],
    processPartition: (TaskContext, Iterator[T]) => U,
    resultHandler: (Int, U) => Unit)
  {
    runJob[T, U](rdd, processPartition, 0 until rdd.partitions.size, false, resultHandler)
  }

  /**
   * Run a job on all partitions in an RDD and pass the results to a handler function.
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      resultHandler: (Int, U) => Unit)
  {
    val processFunc = (context: TaskContext, iter: Iterator[T]) => processPartition(iter)
    runJob[T, U](rdd, processFunc, 0 until rdd.partitions.size, false, resultHandler)
  }

  /**
   * :: DeveloperApi ::
   * Run a job that can return approximate results.
   */
  @DeveloperApi
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      timeout: Long): PartialResult[R] = {
//    val callSite = getCallSite
//    logInfo("Starting job: " + callSite.shortForm)
//    val start = System.nanoTime
//    val result = dagScheduler.runApproximateJob(rdd, func, evaluator, callSite, timeout,
//      localProperties.get)
//    logInfo(
//      "Job finished: " + callSite.shortForm + ", took " + (System.nanoTime - start) / 1e9 + " s")
//    result
    null
  }

  /**
   * :: Experimental ::
   * Submit a job for execution and return a FutureJob holding the result.
   */
  @Experimental
  def submitJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R): SimpleFutureAction[R] =
  {
//    val cleanF = clean(processPartition)
//    val callSite = getCallSite
//    val waiter = dagScheduler.submitJob(
//      rdd,
//      (context: TaskContext, iter: Iterator[T]) => cleanF(iter),
//      partitions,
//      callSite,
//      allowLocal = false,
//      resultHandler,
//      localProperties.get)
//    new SimpleFutureAction(waiter, resultFunc)
    null
  }

  /**
   * Cancel active jobs for the specified group. See [[org.apache.spark.SparkContext.setJobGroup]]
   * for more information.
   */
  def cancelJobGroup(groupId: String) {
    //dagScheduler.cancelJobGroup(groupId)
  }

  /** Cancel all jobs that have been scheduled or are running.  */
  def cancelAllJobs() {
    //dagScheduler.cancelAllJobs()
  }

  /** Cancel a given job if it's scheduled or running */
  private def cancelJob(jobId: Int) {
    //dagScheduler.cancelJob(jobId)
  }

  /** Cancel a given stage and all jobs associated with it */
  private def cancelStage(stageId: Int) {
    //dagScheduler.cancelStage(stageId)
  }

  /**
   * Clean a closure to make it ready to serialized and send to tasks
   * (removes unreferenced variables in $outer's, updates REPL variables)
   * If <tt>checkSerializable</tt> is set, <tt>clean</tt> will also proactively
   * check to see if <tt>f</tt> is serializable and throw a <tt>SparkException</tt>
   * if not.
   *
   * @param f the closure to clean
   * @param checkSerializable whether or not to immediately check <tt>f</tt> for serializability
   * @throws <tt>SparkException<tt> if <tt>checkSerializable</tt> is set but <tt>f</tt> is not
   *   serializable
   */
  private def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    ClosureCleaner.clean(f, checkSerializable)
    f
  }

  /**
   * Set the directory under which RDDs are going to be checkpointed. The directory must
   * be a HDFS path if running on a cluster.
   */
  def setCheckpointDir(directory: String):Unit = {
    //    checkpointDir = new Some(directory).map { dir =>
    //      val path = new Path(dir, UUID.randomUUID().toString)
    //      val fs = path.getFileSystem(hadoopConfiguration)
    //      fs.mkdirs(path)
    //      fs.getFileStatus(path).getPath.toString
    //    }
//    checkpointDir = directory
    val path:Path = new Path(directory, UUID.randomUUID().toString)
    val fs:FileSystem = path.getFileSystem(new YarnConfiguration)
    fs.mkdirs(path)
    fs.getFileStatus(path).getPath().toString()
    
  }

  def getCheckpointDir():Option[String] = {
    new Some("")
    //checkpointDir
  }

  /** Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD). */
  def defaultParallelism: Int = 1

  /** Default min number of partitions for Hadoop RDDs when not given by user */
  @deprecated("use defaultMinPartitions", "1.0.0")
  def defaultMinSplits: Int = 1

  /** Default min number of partitions for Hadoop RDDs when not given by user */
  def defaultMinPartitions: Int = 2

//  private val nextShuffleId = new AtomicInteger(0)

//  private def newShuffleId(): Int = nextShuffleId.getAndIncrement()

  private val nextRddId = new AtomicInteger(0)

  /** Register a new RDD, returning its RDD ID */
  private def newRddId(): Int = nextRddId.getAndIncrement()

//  /** Post the application start event */
//  private def postApplicationStart() {
//    listenerBus.post(SparkListenerApplicationStart(appName, startTime, sparkUser))
//  }
//
//  /** Post the application end event */
//  private def postApplicationEnd() {
//    listenerBus.post(SparkListenerApplicationEnd(System.currentTimeMillis))
//  }
//
//  /** Post the environment update event once the task scheduler is ready */
//  private def postEnvironmentUpdate() {
////    if (taskScheduler != null) {
////      val schedulingMode = getSchedulingMode.toString
////      val addedJarPaths = addedJars.keys.toSeq
////      val addedFilePaths = addedFiles.keys.toSeq
////      val environmentDetails =
////        SparkEnv.environmentDetails(conf, schedulingMode, addedJarPaths, addedFilePaths)
////      val environmentUpdate = SparkListenerEnvironmentUpdate(environmentDetails)
////      listenerBus.post(environmentUpdate)
////    }
//  }

//  /** Called by MetadataCleaner to clean up the persistentRdds map periodically */
//  private def cleanup(cleanupTime: Long) {
//    persistentRdds.clearOldValues(cleanupTime)
//  }
}

/**
 * The SparkContext object contains a number of implicit conversions and parameters for use with
 * various Spark features.
 */
