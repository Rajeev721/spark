# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC Apache Spark began at UC Berkeley in 2009 as the Spark research project by Matei Zaharia.
# MAGIC 
# MAGIC At the time, Hadoop MapReduce was the dominant parallel programming engine. But MapReduce framework has drawbacks as it has to store intermediate data on disk and this will impact the performance of applications.
# MAGIC 
# MAGIC Apache Spark is a unified computing engine and a set of libraries for parallel data processing on computer clusters.
# MAGIC 
# MAGIC * Unified: Can do variety of tasks such as simple data loading, SQL queries for analyzing data, machine learning, stream data processing etc.
# MAGIC * Computing engine: Spark handles loading data from storage systems and performs computation on it
# MAGIC * Libraries: Spark includes libraries for SQL and structured data (Spark SQL), machine learning (MLlib), stream processing (Spark Streaming and the newer Structured Streaming), and graph analytics (GraphX).
# MAGIC 
# MAGIC PySpark is a Python API for Apache Spark. Apache Spark is an processing engine for large scale powerful distributed data processing and machine learning applications.
# MAGIC 
# MAGIC Spark basically written in Scala and later on due to its industry adaptation it’s API PySpark released for Python using Py4J. Py4J is a Java library that is integrated within PySpark and allows python to dynamically interface with JVM objects, hence to run PySpark you also need Java to be installed along with Python, and Apache Spark.
# MAGIC 
# MAGIC From Spark 2.0 SparkSession has become an entry point to PySpark to work with RDD, DataFrame. Prior to 2.0, SparkContext used to be an entry point. In PySpark Shell we will be having a default spark session object "spark".
# MAGIC 
# MAGIC SparkSession is a combined class for all different contexts we used to have prior to 2.0 relase (SQLContext and HiveContext e.t.c). Since 2.0 SparkSession can be used in replace with SQLContext, HiveContext, and other contexts defined prior to 2.0
# MAGIC 
# MAGIC SparkSession internally creates SparkConfig and SparkContext with the configuration provided with SparkSession.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Spark Architecture:
# MAGIC 
# MAGIC Spark manages and coordinates the execution of tasks on data across a cluster of computers.
# MAGIC 
# MAGIC * Driver Program – Manages the Spark application and assigns the tasks to executors.
# MAGIC * Cluster Manager – Manages the cluster resources and grants resources to submitted Spark application
# MAGIC * Executors – Will do the actual work 
# MAGIC 
# MAGIC https://i2.wp.com/sparkbyexamples.com/wp-content/uploads/2020/02/spark-cluster-overview.png?w=596&ssl=1
# MAGIC 
# MAGIC 
# MAGIC Spark application consists Driver process and Executor processes,
# MAGIC 
# MAGIC * Driver process:
# MAGIC     Driver process runs your main() function and it sits on a node in the cluster
# MAGIC     It’s the heart of a Spark application and maintains all the information about your application
# MAGIC     This process is responsible for,
# MAGIC     Managing information about your Spark application
# MAGIC     Responding to user’s program or input
# MAGIC     Distributing and scheduling the work across the executors
# MAGIC     
# MAGIC * Executor process:
# MAGIC     Executor processes will do the actual work and sits on worker nodes in a cluster.
# MAGIC     They are responsible for, Executing the code assigned by the Driver Reporting the state of computation back to the Driver
# MAGIC 
# MAGIC Cluster Manager Types
# MAGIC 
# MAGIC As of writing this Spark with Python (PySpark) tutorial, Spark supports below cluster managers:
# MAGIC 
# MAGIC     Standalone – a simple cluster manager included with Spark that makes it easy to set up a cluster.
# MAGIC     Apache Mesos – Mesons is a Cluster manager that can also run Hadoop MapReduce and PySpark applications.
# MAGIC     Hadoop YARN – the resource manager in Hadoop 2. This is mostly used, cluster manager.
# MAGIC     Kubernetes – an open-source system for automating deployment, scaling, and management of containerized applications.
# MAGIC 
# MAGIC local – which is not really a cluster manager but still I wanted to mention as we use “local” for master() in order to run Spark on your laptop/computer.

# COMMAND ----------

from pyspark.sql import SparkSession
rajeev = SparkSession.builder.master("yarn").appName("PySpark_RDD_Basic").getOrCreate()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC RDD represents an immutable, partitioned collection of records that can be operated on in parallel.
# MAGIC 
# MAGIC RDDs operate in parallel. This is the strongest advantage of working in Spark: Each transformation is executed in parallel for enormous increase in speed.
# MAGIC 
# MAGIC The transformations to the dataset are lazy. This means that any transformation is only executed when an action on a dataset is called. This helps Spark to optimize the execution.
# MAGIC 
# MAGIC As an end user, you will create following 2 types of RDDs:
# MAGIC 
# MAGIC 1) Generic RDD
# MAGIC 2) Key-Value RDD
# MAGIC     
# MAGIC We can create RDDs in following ways:
# MAGIC 
# MAGIC 1) From a Collection
# MAGIC 2) From Data Sources
# MAGIC 3) From Data Frame
# MAGIC 4) From a Existing RDD
# MAGIC   

# COMMAND ----------

#Creating RDD from a collection:
# we need to create a SparkContext while working with RDDs and it has already been created in this note book by default
# We can create a SparkContext from a SparkSession using below statement
#sparckContext = sparksession object.sparckContext
sc = rajeev.sparkContext
print(sc)

rdd = sc.parallelize([(12, 20, 35, 'a b c'), (41, 58, 64, 'd e f'),  (70, 85, 90, 'g h i')])
print(type(rdd))
rdd.collect()

# COMMAND ----------

rdd.take(2)

# COMMAND ----------

#Creating RDD from a Data Source

rdd_ds = sc.textFile('dbfs:/FileStore/tables/alice_in_wonderland.txt')
rdd_ds.count()

# COMMAND ----------

#From DataFrame

df = sc.parallelize([(12, 20, 35, 'a b c'),  (41, 58, 64, 'd e f'),  (70, 85, 90, 'g h i')]).toDF(['col1', 'col2', 'col3','col4'])
print(type(df))
print("number of records in Data Frame: ", df.count())

#Converting a DataFrame into RDD
rdd_df = df.rdd
print(type(rdd_df))
print("number of records in RDD: ", rdd_df.count())
rdd_df.collect()

# COMMAND ----------

#From an Existing RDD

rdd = sc.parallelize([i for i in range(100)])
print(rdd.take(10))
rdd_new = rdd.filter( lambda x : x >20)
rdd_new.take(10)

# COMMAND ----------

# Key Value RDD
# When called on a RDD of (K, V) pairs, returns new RDD of (K, Iterable<V>) pairs.

RDD = sc.parallelize([("Apple",10), ("Banana",5), ("Banana",2), ("Apple",3), ("Apple",25)])

print(RDD.getNumPartitions())
print(type(RDD))
groupedRDD = RDD.groupByKey()
print(type(groupedRDD))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC RDD transformations – Transformations are lazy operations. When you run a transformation(for example update), instead of updating a current RDD, these operations return another RDD.
# MAGIC 
# MAGIC Sample Transformations:
# MAGIC 
# MAGIC     map
# MAGIC     flatMap
# MAGIC     filter
# MAGIC     distinct
# MAGIC     reduceByKey
# MAGIC     mapPartitions
# MAGIC     sortBy
# MAGIC 
# MAGIC Transformations are the core of how you express your business logic using Spark.
# MAGIC 
# MAGIC There are 2 types of transformations
# MAGIC *	Narrow Transformations : Each input partition will contribute to only one output partition. No shuffling happens in narrow transformations.
# MAGIC *	Wide Transformations : Input partition contributes to multiple output partitions. Shuffling happens in wide transformations.
# MAGIC 
# MAGIC RDD actions – operations that trigger computation and return RDD values to the driver.
# MAGIC 
# MAGIC 
# MAGIC     collect
# MAGIC     collectAsMap
# MAGIC     reduce
# MAGIC     countByKey/countByValue
# MAGIC     take
# MAGIC     first

# COMMAND ----------

rdd = sc.parallelize([i for i in range(10)])

rdd_map = rdd.map(lambda x : x**2)

print(rdd_map.collect())
print(rdd_map.take(5))

RDD = sc.parallelize([("Apple",10), ("Banana",5), ("Banana",2), ("Apple",3), ("Apple",25)])
print(RDD.collectAsMap())


# COMMAND ----------

RDD_Wide = sc.parallelize([("Apple",10), ("Banana",5), ("Banana",3), ("Apple",25)])
CountRDD = RDD_Wide.reduceByKey(lambda v1,v2: v1+v2)
CountRDD.collect()

# COMMAND ----------

#join Transformation on RDD
rdd1 = sc.parallelize([('Banana', 7), ('Apple', 38), ('Cherry', 25)])
rdd2 = sc.parallelize([('Apple', 5), ('Cherry', 8), ('Durian', 4)])

# Inner join
joinRDD = rdd1.join(rdd2) # [('Cherry', (25, 8)), ('Apple', (38, 5))]

# Left Outer Join
leftJoinRDD = rdd1.leftOuterJoin(rdd2) # [('Cherry', (25, 8)), ('Banana', (7, None)), ('Apple', (38, 5))]

# Right Outer Join
rightJoinRDD = rdd1.rightOuterJoin(rdd2) # [('Cherry', (25, 8)), ('Apple', (38, 5)), ('Durian', (None, 4))]

# Full Outer Join
fullJoinRDD = rdd1.fullOuterJoin(rdd2) # [('Cherry', (25, 8)), ('Durian', (None, 4)), ('Banana', (7, None)), ('Apple', (38, 5))]

print(joinRDD.collect())
print(leftJoinRDD.collect())
print(rightJoinRDD.collect())
print(fullJoinRDD.collect())

# COMMAND ----------

rdd = sc.parallelize([i for i in range(10000)])
print(rdd.getNumPartitions())
rdd1 = rdd.coalesce(3)
print(rdd1.getNumPartitions())
rdd2 = rdd.repartition(8)
print(rdd2.getNumPartitions())

# COMMAND ----------

pairs  = sc.parallelize([["a",1], ["b",2], ["c",3], ["d",3], ["a",4], ["a",5]])

pairs.collect() 
# [['a', 1], ['b', 2], ['c', 3], ['d', 3], ['a', 4], ['a', 5]]

# Repartitions to 2 and Sorts the data within partitions
print(pairs.repartitionAndSortWithinPartitions(2).glom().collect())
# [[('a', 1), ('a', 4), ('a', 5), ('c', 3)], [('b', 2), ('d', 3)]]

# Repartitions to 2 based on given partitioner function and Sorts the data within partitions
pairs.repartitionAndSortWithinPartitions(2, partitionFunc=lambda x: x == 'a').glom().collect()
# [[('b', 2), ('c', 3), ('d', 3)], [('a', 1), ('a', 4), ('a', 5)]]


# COMMAND ----------

rdd1 = sc.parallelize([1,2,3,4])
rdd2 = sc.parallelize([1,3])

# union: Return a new dataset that contains the union of the elements in the source dataset and the argument.
unionRDD = rdd1.union(rdd2) # [1, 2, 3, 4, 1, 3]

print(unionRDD.collect())

#intersection: Return a new RDD that contains the intersection of elements in the source dataset and the argument.

intersectionRDD = rdd1.intersection(rdd2)

print(intersectionRDD.collect())

# distinct: Return a new dataset that contains the distinct elements of the source dataset

distinctRDD = rdd1.distinct()

print(distinctRDD.collect())
