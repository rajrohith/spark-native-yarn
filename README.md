STARK - Spark on Tez
============

This project represents a prototype of running DAGs assembled using SPARK API on Apache Tez
It is dependent on modifications to SPARK code described int [SPARK-3561](https://issues.apache.org/jira/browse/SPARK-3561). 
This means that to use it, one must have a custom build of Spark which incorporates pending [GitHub Pull Request](https://github.com/apache/spark/pull/2422).
Below are the directions on how to get started.

#### Checkout and Build SPARK-3561
```
$> git clone https://github.com/olegz/spark-1.git
$> cd spark-1
$> git fetch --all
$> git branch --track SPARK-HADOOP origin/SPARK-HADOOP
$> git checkout SPARK-HADOOP
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
You should see a successful build
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

This completes pre-requisite required to run STARK

#### STARK 
Aside from enabling SPARK DAG execution to run on Tez, this project provides additional functionality which addresses developer productivity including but not limited to:
 * _executing your code from the IDE (Eclipse and or Idea)_
 * _transparent classpath management_ 
 * _integration with mini-cluster environment_ 
 
Details are described below.

##### Checkout and build STARK
```
$> git clone https://github.com/hortonworks/spark-on-tez.git
$> cd spark-on-tez
```
##### Setup STARK development environment
Eclipse
```
$> ./gradlew clean eclipse
```
IntelliJ Idea
```
$> ./gradlew clean idea
```
Import projects into your workspace as regular project (not maven or gradle project) as all required artifacts were already generated.
For example in Eclipse:
```
File -> Import -> General -> Existing Project into Workspace 
```
Your code 
