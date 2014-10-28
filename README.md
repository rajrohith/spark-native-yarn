### spark-native-yarn 

#### Native YARN integration with Apache Spark
============

> IMPORTANT: At the time or writing, the project represents a **_prototype_** with the goal of demonstrating 
the validity of the approach described in [SPARK-3561](https://issues.apache.org/jira/browse/SPARK-3561). 
To get an idea of currently supported functionality please refer to 
[APIDemoTests](https://github.com/hortonworks/spark-native-yarn/blob/master/src/test/scala/org/apache/spark/tez/APIDemoTests.scala)

==

**_spark-native-yarn_** project represents an extension to [Apache Spark](https://spark.apache.org/) which enables DAGs assembled using SPARK API to run on [Apache Tez](http://tez.apache.org/),
thus allowing one to benefit from native features of Tez, especially related to large scale Batch/ETL applications.

Aside from enabling SPARK DAG execution to run on [Apache Tez](http://tez.apache.org/), this project provides additional functionality which addresses developer productivity including but not limited to:
 * _executing your code on YARN cluster directly from the IDE (Eclipse and/or IntelliJ)_
 * _remote submission (submission from the remote client)_
 * _transparent classpath management_ 
 * _seamless and simplified integration with mini-cluster environment_ 
 * _enhanced debugging capabilities ability to place and step thru the breakpoints in SPARK application code when using mini-cluster (see InJvmContainerExecutor provided with [mini-dev-cluster](https://github.com/hortonworks/mini-dev-cluster))_
 * _ability to utilize [Tez local mode](http://tez.apache.org/localmode.html)
 
At the moment of writing, _**spark-native-yarn**_ is dependent on modifications to SPARK code described in [SPARK-3561](https://issues.apache.org/jira/browse/SPARK-3561). 
This means that to use it, one must have a custom build of Spark which incorporates pending [GitHub Pull Request](https://github.com/apache/spark/pull/2849).
You can build your own by following instructions below or you can download a pre-built distribution from [here](jjjj).

> IMPORTANT: If you opt out for a pre-build distribution keep in mind that it is based on Spark 1.1 release, which means you have to use a compatible **_spark-native-yarn_**
version [branch 1.1.1](https://github.com/hortonworks/spark-native-yarn/tree/1.1.1).

For those who want to take their chances with the latest Spark's snapshot, please follow instructions below, otherwise skip and go straight to 
[build spark-native-yarn](https://github.com/hortonworks/spark-native-yarn/tree/master#clone-spark-native-yarn)


Below are the prerequisites and instructions on how to proceed.

> IMPORTANT: Please follow the prerequisites described below and then continue to [**_Getting Started_**](https://github.com/hortonworks/spark-native-yarn/wiki/Home) guide.

#### Checkout and Build SPARK-3561
```
$> git clone https://github.com/olegz/spark-1.git
$> cd spark-1
$> git fetch --all
```

Switch to SPARK-3561 branch

```
$> git branch --track SH-1 origin/SH-1
$> git checkout SH-1
```
Spark uses Maven for its build so it must be present. And to ensure there are no OOM errors set up Maven options as below. 
See [Spark's documentation](https://spark.apache.org/docs/latest/building-with-maven.html) for more details.

```
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
```

##### Build and install SPARK-3561 into your local maven repository

```
$> mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests clean install
```
The build should take 20-30 min depending on your machine. You should see a successful build
```
INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary:
[INFO]
[INFO] Spark Project Parent POM .......................... SUCCESS [  2.281 s]
[INFO] Spark Project Core ................................ SUCCESS [02:33 min]
[INFO] Spark Project Bagel ............................... SUCCESS [ 18.959 s]
. . .
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

##### Clone spark-native-yarn

```
$> git clone https://github.com/hortonworks/spark-native-yarn.git
$> cd spark-native-yarn
```

To switch to 1.1.1 branch:

```
$> git fetch --all
$> git branch --track 1.1.1 origin/1.1.1
$> git checkout SH-1
```

This completes the pre-requisite required to run STARK and you can now 
continue to [**_Getting Started_**](https://github.com/hortonworks/spark-native-yarn/wiki/Home) guide.

==



