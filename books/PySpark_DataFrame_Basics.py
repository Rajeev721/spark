# Databricks notebook source
# MAGIC %md PySpark DataFrame Basics
# MAGIC 
# MAGIC * DataFrame is equivalant to a table in DataBase and DataFrame in Pandas in python.
# MAGIC * DataFrames are (distributed) table-like collections with well-defined rows and columns.
# MAGIC * Each column has type information that must be consistent for every row in the collection.
# MAGIC * When you’re using DataFrames, you’re taking advantage of Spark’s optimized internal format.
# MAGIC * Spark has its own datatypes. 
# MAGIC * Internally, Spark uses an engine called Catalyst that maintains its own type information through the planning and processing of work.
# MAGIC * Spark types map directly to the different language APIs that Spark maintains and there exists a lookup table for each of these in Scala, Java, Python, SQL, and R.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.Builder()\
      .master("yarn")\
      .appName("PySpark_DataFrame_Basics")\
      .getOrCreate()

print(f"Spark Context is {spark} and type is {type(spark)}")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC We can create DataFrames using below methods:
# MAGIC 
# MAGIC * 1) Using RDD
# MAGIC * 2) Using Different Data Sources
# MAGIC * 3) Using an Existing DataFrame

# COMMAND ----------

#Using RDD

RDD = sc.parallelize([("Apple",10), ("Banana",5), ("Banana",2), ("Apple",3), ("Apple",25)])

schema = ["Name", "Count"]

df = spark.createDataFrame(RDD,schema)
df.show()
df.printSchema()

# COMMAND ----------

#Using Different Data Sources
#CSV
DataFrame = spark.read.csv('dbfs:/FileStore/tables/singapore_residents_by_age_group_ethnic_group_and_sex_end_june_annual.csv', header=True,\
                           inferSchema =True)
print(DataFrame.count())
print(DataFrame.take(10))


# COMMAND ----------

print(df.dtypes)
print(df.show())
print(df.head())
print(df.first())
print(df.take(2))
print(df.schema)

# COMMAND ----------

DataFrame.describe().show()

# COMMAND ----------

RDD = sc.parallelize([("Apple",10), ("Banana",2), ("Banana",2), ("Apple",10), ("Apple",25)])
schema = ["Name", "Count"]

df = spark.createDataFrame(RDD,schema)
df.show()

#Drop Duplicates
df1 = df.dropDuplicates()
df1.show()

# COMMAND ----------

# In DataFrames, Spark internally maintains the datatypes and only checks whether those types line up to those specified in the schema at runtime.
Number = StructField('Number',IntegerType(),True)
Name = StructField('Name',StringType(),True)
c_list = [Number,Name]
csv_schema=StructType(c_list)

df_csv = spark.read.csv('dbfs:/FileStore/sample.csv',schema=csv_schema)
df_csv.show()

# COMMAND ----------

#Creating a table from DataFrame
DataFrame.createOrReplaceTempView("singapore")

spark.sql("select * from singapore limit 2").show()

# COMMAND ----------

# By using col() or column() functions, we can refer the columns of a dataframe.

from pyspark.sql.functions import col, column

# Using col, column functions
print(DataFrame.select(col("year")).take(2))
print(DataFrame.select(column("year")).take(2))
print(DataFrame.select("value").take(2))


# COMMAND ----------

#using expression

from pyspark.sql.functions import expr

print(DataFrame.select(expr ("level_2 as duration")).take(2))

# COMMAND ----------

# lit() function translates the value from a given programming language to one that Spark understands

from pyspark.sql.functions import lit

df_1 = DataFrame.select(expr("*"), lit(1).alias("NewCol"))
df_1.printSchema()

# COMMAND ----------

df_2 = df_1.drop(df_1.level_1)
df_2.show()

# COMMAND ----------

#Changing the DataType of a column

df_2.printSchema()

df3 = df_2.select("year",col("value").cast('int'))

#df = df1.join(df2, df1.c1 == df2.c2, how = '')

df3.printSchema()

# COMMAND ----------

#filtering the data
df3.where(col("year")>1957).take(5)


# COMMAND ----------

df3.select("year").orderBy("year").distinct().show(5)

# COMMAND ----------

#Repartition and Coalesce

print(df3.rdd.getNumPartitions())
df3.repartition(8)
print(df3.rdd.getNumPartitions())

# COMMAND ----------

#Working with Booleans
df3.where(col("year") != 1957).select("year", "value").show(10,False)

# COMMAND ----------

df3.where(col("year") != 1957).select("year", "value").show(10,True)

# COMMAND ----------


