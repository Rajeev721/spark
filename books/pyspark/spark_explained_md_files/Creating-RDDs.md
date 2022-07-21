There are two ways to create an RDD in PySpark: you can either .parallelize(...) a collection (list or an array of some elements):
>    data = sc.parallelize([('Amber', 22), ('Alfred', 23), ('Skye',4), ('Albert', 12), ('Amber', 9)])

Object holding data type - **ParallelCollectionRDD**

***

Or you can reference a file (or files) located either locally or somewhere externally:
>    data_from_file = sc. textFile('/Users/drabast/Documents/VS14MORT.txt.gz',4)

Object holding data type - **MapPartitionsRDD**
The last parameter in sc.textFile(..., n) specifies the number of partitions the dataset is divided into.

***

### Schema
RDDs are schema-less data structures (unlike DataFrames, which we will discuss in the next). Thus, parallelizing a dataset, such as in the following code snippet, is perfectly fine with Spark when using RDDs:

> data_heterogenous = sc.parallelize([
>     ('Ferrari', 'fast'),
>     {'Porsche': 100000},
>     ['Spain','visited', 4504]
> ]).collect()

So, we can mix almost anything: a tuple, a dict, or a list and Spark will not complain.

Once you .collect() the dataset (that is, run an action to bring it back to the driver) you can access the data in the object as you would normally do in Python:

> data_heterogenous[1]['Porsche']
> 100000

The .collect() method returns all the elements of the RDD to the driver where it is serialized as a list.