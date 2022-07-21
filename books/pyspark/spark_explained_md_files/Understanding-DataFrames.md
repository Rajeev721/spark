A DataFrame is an immutable distributed collection of data that is organized into named columns analogous to a table in a relational database. Introduced as an experimental feature within Apache Spark 1.0 as SchemaRDD, they were renamed to DataFrames as part of the Apache Spark 1.3 release. For readers who are familiar with Python Pandas DataFrame or R DataFrame, a Spark DataFrame is a similar concept in that it allows users to easily work with structured data (for example, data tables); there are some differences as well so please temper your expectations.

By imposing a structure onto a distributed collection of data, this allows Spark users to query structured data in Spark SQL or using expression methods (instead of lambdas)

![df](https://www.safaribooksonline.com/library/view/learning-pyspark/9781786463708/graphics/B05793_03_02.jpg)

### Performance

![performance](https://www.safaribooksonline.com/library/view/learning-pyspark/9781786463708/graphics/B05793_03_03.jpg)

### Dataframe in PySpark

In Apache Spark, a DataFrame is a distributed collection of rows under named columns. In simple terms, it is same as a table in relational database or an Excel sheet with Column headers. It also shares some common characteristics with RDD:

* Immutable in nature : We can create DataFrame / RDD once but can’t change it. And we can transform a DataFrame / RDD  after applying transformations.
* Lazy Evaluations: Which means that a task is not executed until an action is performed.
* Distributed: RDD and DataFrame both are distributed in nature.

### Why DataFrames are Useful ?

* DataFrames are designed for processing large collection of structured or semi-structured data.
* Observations in Spark DataFrame are organised under named columns, which helps Apache Spark to understand the schema of a * DataFrame. This helps Spark optimize execution plan on these queries.
* DataFrame in Apache Spark has the ability to handle petabytes of data.
* DataFrame has a support for wide range of data format and sources.
* It has API support for different languages like Python, R, Scala, Java.
 
![](https://www.analyticsvidhya.com/wp-content/uploads/2016/10/DataFrame-in-Spark.png)

### Important classes of Spark SQL and DataFrames:

* **pyspark.sql.SQLContext** Main entry point for DataFrame and SQL functionality.
* **pyspark.sql.DataFrame** A distributed collection of data grouped into named columns.
* **pyspark.sql.Column** A column expression in a DataFrame.
* **pyspark.sql.Row** A row of data in a DataFrame.
* **pyspark.sql.GroupedData** Aggregation methods, returned by DataFrame.groupBy().
* **pyspark.sql.DataFrameNaFunctions** Methods for handling missing data (null values).
* **pyspark.sql.DataFrameStatFunctions** Methods for statistics functionality.
* **pyspark.sql.functions** List of built-in functions available for DataFrame.
* **pyspark.sql.types** List of data types available.
* **pyspark.sql.Window** For working with window functions.

https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html

### Various Types of deployment
* YARN as cluster manager - Don't come inbuilt
* Mesos as cluster manager - Don't come inbuilt
* Spark Standalone - This one is provided by spark & comes bundled with Spark

##### master
URL of cluster, suppose everything is running in local machine - "local[4]" (run locally with 4 cores)
spark://master:7077 - You have a standalone cluster whose url is this.

##### config
This guy will contain all key-value configuration pair
SparkSession.builder.config(conf=SparkConf())
config("info","great")

#### sparkSession - spark
Wapper around SqlContext 
#### SparkContext - sc
Entry point to entire spark
#### SqlContext - sqlContext
Entry point for working with structured data