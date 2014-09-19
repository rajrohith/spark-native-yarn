spark-on-tez
============

This project represents a prototype of running DAGs assembled using SPARK API on Apache Tez
It is dependent on modifications to SPARK code described [here](https://issues.apache.org/jira/browse/SPARK-3561). 
This means that to use it, one must have a custom build of Spark which incorporates pending [Pull Request](https://github.com/apache/spark/pull/2422)
