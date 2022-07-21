### How Can One Use DataFrames?

Once built, DataFrames provide a domain-specific language for distributed data manipulation.  Here is an example of using DataFrames to manipulate the demographic data of a large population of users:

    # Create a new DataFrame that contains “young users” only
    young = users.filter(users.age < 21)

    # Alternatively, using Pandas-like syntax
    young = users[users.age < 21]

    # Increment everybody’s age by 1
    young.select(young.name, young.age + 1)

    # Count the number of young users by gender
    young.groupBy(“gender”).count()

    # Join young users with another DataFrame called logs
    young.join(logs, logs.userId == users.userId, “left_outer”)

You can also incorporate SQL while working with DataFrames, using Spark SQL. This example counts the number of users in the young DataFrame.

    young.registerTempTable(“young”)
    context.sql(“SELECT count(*) FROM young”)

In Python, you can also convert freely between Pandas DataFrame and Spark DataFrame:

    # Convert Spark DataFrame to Pandas
    pandas_df = young.toPandas()

    # Create a Spark DataFrame from Pandas
    spark_df = context.createDataFrame(pandas_df)

**Disclaimer:  A few operations that you can do in Pandas don’t translate to Spark well. Please remember that DataFrames in Spark are like RDD in the sense that they’re an immutable data structure. Therefore things like:**

    # to create a new column "three"
    df[‘three’] = df[‘one’] * df[‘two’]

* Can’t exist, just because this kind of affectation goes against the principles of Spark. Another example would be trying to access by index a single element within a DataFrame. 
* Don’t forget that you’re using a distributed data structure, not an in-memory random-access data structure.

https://databricks.com/blog/2015/08/12/from-pandas-to-apache-sparks-dataframe.html
