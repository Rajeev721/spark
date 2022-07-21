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

***

### class pyspark.sql.SparkSession(sparkContext, jsparkSession=None)
The entry point to programming Spark with the Dataset and DataFrame API. A SparkSession can be used create DataFrame, register DataFrame as tables, execute SQL over tables, cache tables, and read parquet files. 

