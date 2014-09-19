spark-on-tez
============

This project represents a prototype of running DAGs assembled using SPARK API on Apache Tez
It is dependent on modifications to SPARK code described [here](https://issues.apache.org/jira/browse/SPARK-3561). 
This means that to use it, one must have a custom build of Spark which incorporates pending [Pull Request](https://github.com/apache/spark/pull/2422).
Below are the directions on how to get started.

**_Checkout and Build SPARK-3561_**
```
$> git clone https://github.com/olegz/spark-1.git
$> cd spark-1
```
Spark uses Maven as for its build so it must be present

```
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

```

The above will ensure there is no OOM errors during build. For more details see [Spark's documentation](https://spark.apache.org/docs/latest/building-with-maven.html)

