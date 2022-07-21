![](https://github.com/awantik/pyspark-tutorial/blob/master/zekeLabs_Logo.png)

**Transformations shape your dataset. These include mapping, filtering, joining, and transcoding the values in your dataset. In this section, we will showcase some of the transformations available on RDDs.**

### Concepts
* Action & Transformation
* Combiner
* Driver Program
* Executor
* Job
* Lazy Evaluation
* Lineage Graph
* Modes of Operation
* RDD
* RDD Persistence
* RDD Types
* Serialization
* Stage
* Task

### Initialization
* cache()
* persist()
* SparkConf
* parallelize()
* textFile()

### Action (RDD)
* aggregate()
* collect()
* count()
* countByValue()
* first()
* fold()
* foreach()
* reduce()
* saveAsTextFile()
* take()
* takeOrdered()
* takeSample()
* top()

### Action (PairRDDFunctions)
* collectAsMap()
* countByKey()
* lookup()

### Map Operations
* flatMap()
* map()
* mapPartitions()
* mapPartitionsWithIndex()

### Set Operations
* cartesian()
* distinct()
* intersection()
* subtract()
* union()

### Other Operations
* filter()
* groupBy()
* toDebugString

### PairRDDFunctions (Single RDD)
* combineByKey()
* foldByKey()
* groupByKey()
* mapValues()
* reduceByKey()

### PairRDDFunctions (Two RDD)
* cogroup()
* join()
* leftOuterJoin()
* rightOuterJoin()

### Sorting
* sortByKey()
* takeOrdered()
* top()

### Partition
* General
* Hash-Partition
* Partitioner set Operations
* Partitioner unset Operations
* range-partition
* Shuffling
* coalesce()
* partitionBy()
* repartition()

### Shared Variables
* Accumlators
* Broadcast Variable

***

#### cache()
Persist this RDD with the default storage level (MEMORY_ONLY).

#### cartesian(other)
Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of elements (a, b) where a is in self and b is in other.

     rdd = sc.parallelize([1, 2])
     sorted(rdd.cartesian(rdd).collect())
     [(1, 1), (1, 2), (2, 1), (2, 2)]

#### glom()
Create array for data in different partitions.

#### coalesce(numPartitions, shuffle=False)
Return a new RDD that is reduced into numPartitions partitions.

     sc.parallelize([1, 2, 3, 4, 5], 3).glom().collect()
     [[1], [2, 3], [4, 5]]
     sc.parallelize([1, 2, 3, 4, 5], 3).coalesce(1).glom().collect()
     [[1, 2, 3, 4, 5]]

#### collect()
Return a list that contains all of the elements in this RDD.

_Note This method should only be used if the resulting array is expected to be small, as all the data is loaded into the driver’s memory._

#### collectAsMap()
Return the key-value pairs in this RDD to the master as a dictionary.

_Note this method should only be used if the resulting data is expected to be small, as all the data is loaded into the driver’s memory._
     m = sc.parallelize([(1, 2), (3, 4)]).collectAsMap()
     m[1]
     2
     m[3]
     4

#### combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions=None, partitionFunc)
* Generic function to combine the elements for each key using a custom set of aggregation functions.
* Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a “combined type” C.

Users provide three functions:

* createCombiner, which turns a V into a C (e.g., creates a one-element list)
* mergeValue, to merge a V into a C (e.g., adds it to the end of a list)
* mergeCombiners, to combine two C’s into a single one.
In addition, users can control the partitioning of the output RDD.

Note V and C can be different – for example, one might group an RDD of type (Int, Int) into an RDD of type (Int, List[Int]).

     x = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
     def add(a, b): return a + str(b)
     sorted(x.combineByKey(str, add, add).collect())
     [('a', '11'), ('b', '1')]

     data = sc.parallelize( [(0, 2.), (0, 4.), (1, 0.), (1, 10.), (1, 20.)] )
     sumCount = data.combineByKey(lambda value: (value, 1),
                             lambda x, value: (x[0] + value, x[1] + 1),
                             lambda x, y: (x[0] + y[0], x[1] + y[1]))
     averageByKey = sumCount.map(lambda (label, (value_sum, count)): (label, value_sum / count))
     print averageByKey.collectAsMap()

aggregateByKey() is almost identical to reduceByKey() (both calling combineByKey() behind the scenes), except you give a starting value for aggregateByKey()

#### count()
Return the number of elements in this RDD.

     sc.parallelize([2, 3, 4]).count()
     3

#### countByKey()
Count the number of elements for each key, and return the result to the master as a dictionary.

     rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
     sorted(rdd.countByKey().items())
     [('a', 2), ('b', 1)]

#### countByValue()
Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.

     sorted(sc.parallelize([1, 2, 1, 2, 2], 2).countByValue().items())
     [(1, 2), (2, 3)]

#### distinct(numPartitions=None)
Return a new RDD containing the distinct elements in this RDD.

     sorted(sc.parallelize([1, 1, 2, 3]).distinct().collect())
     [1, 2, 3]

#### filter(f)
Return a new RDD containing only the elements that satisfy a predicate.

     rdd = sc.parallelize([1, 2, 3, 4, 5])
     rdd.filter(lambda x: x % 2 == 0).collect()
     [2, 4]

#### first()
Return the first element in this RDD.

     sc.parallelize([2, 3, 4]).first()
     2
     sc.parallelize([]).first()

#### flatMap(f, preservesPartitioning=False)
Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.

     rdd = sc.parallelize([2, 3, 4])
     sorted(rdd.flatMap(lambda x: range(1, x)).collect())
     [1, 1, 1, 2, 2, 3]
     sorted(rdd.flatMap(lambda x: [(x, x), (x, x)]).collect())
     [(2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)]

#### flatMapValues(f)
Pass each value in the key-value pair RDD through a flatMap function without changing the keys; this also retains the original RDD’s partitioning.

     x = sc.parallelize([("a", ["x", "y", "z"]), ("b", ["p", "r"])])
     def f(x): return x
     x.flatMapValues(f).collect()
     [('a', 'x'), ('a', 'y'), ('a', 'z'), ('b', 'p'), ('b', 'r')]


#### foreach(f)
Applies a function to all elements of this RDD.

     def f(x): print(x)
     sc.parallelize([1, 2, 3, 4, 5]).foreach(f)

#### foreachPartition(f)
Applies a function to each partition of this RDD.

     def f(iterator):
       for x in iterator:
             print(x)
     sc.parallelize([1, 2, 3, 4, 5]).foreachPartition(f)

#### fullOuterJoin(other, numPartitions=None)
Perform a right outer join of self and other.

For each element (k, v) in self, the resulting RDD will either contain all pairs (k, (v, w)) for w in other, or the pair (k, (v, None)) if no elements in other have key k.

Similarly, for each element (k, w) in other, the resulting RDD will either contain all pairs (k, (v, w)) for v in self, or the pair (k, (None, w)) if no elements in self have key k.

Hash-partitions the resulting RDD into the given number of partitions.

     x = sc.parallelize([("a", 1), ("b", 4)])
     y = sc.parallelize([("a", 2), ("c", 8)])
     sorted(x.fullOuterJoin(y).collect())
     [('a', (1, 2)), ('b', (4, None)), ('c', (None, 8))]

#### getCheckpointFile()
Gets the name of the file to which this RDD was checkpointed

Not defined if RDD is checkpointed locally.

#### getNumPartitions()
Returns the number of partitions in RDD

     rdd = sc.parallelize([1, 2, 3, 4], 2)
     rdd.getNumPartitions()
     2

#### getStorageLevel()
Get the RDD’s current storage level.

     rdd1 = sc.parallelize([1,2])
     rdd1.getStorageLevel()

#### StorageLevel(False, False, False, False, 1)
     print(rdd1.getStorageLevel())
Serialized 1x Replicated
glom()
Return an RDD created by coalescing all elements within each partition into a list.

     rdd = sc.parallelize([1, 2, 3, 4], 2)
     sorted(rdd.glom().collect())
     [[1, 2], [3, 4]]

#### groupBy(f, numPartitions=None, partitionFunc=<function portable_hash at 0x7fc35dbc8e60>)
Return an RDD of grouped items.

     rdd = sc.parallelize([1, 1, 2, 3, 5, 8])
     result = rdd.groupBy(lambda x: x % 2).collect()
     sorted([(x, sorted(y)) for (x, y) in result])
     [(0, [2, 8]), (1, [1, 1, 3, 5])]

#### groupByKey(numPartitions=None, partitionFunc=<function portable_hash at 0x7fc35dbc8e60>)
Group the values for each key in the RDD into a single sequence. Hash-partitions the resulting RDD with numPartitions partitions.

Note If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using reduceByKey or aggregateByKey will provide much better performance.

     rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
     sorted(rdd.groupByKey().mapValues(len).collect())
     [('a', 2), ('b', 1)]
     sorted(rdd.groupByKey().mapValues(list).collect())
     [('a', [1, 1]), ('b', [1])]

#### aggregate(zeroValue, seqOp, combOp)
* Compared to reduce() & fold(), the aggregate() function has the advantage, it can return different Type vis-a-vis the RDD Element Type(ie Input Element type)
* In this example, the RDD element type is (String, Int) whereas the return type is Int

    data = sc.parallelize([('awi',4),('bcd',6),('jkl',88),('qek',99)])
    data.aggregate(5,lambda x,y: x + y[1], lambda x,y: x + y)  

#### aggregateByKey(zeroValue, seqFunc, combFunc, numPartitions=None, partitionFunc)
* Aggregate the values of each key, using given combine functions and a neutral “zero value”. 
* This function can return a different result type, U, than the type of the values in this RDD, V. 
* Thus, we need one operation for merging a V into a U and one operation for merging two U’s, The former operation is used for merging values within a partition, and the latter is used for merging values between partitions. 
* To avoid memory allocation, both of these functions are allowed to modify and return their first argument instead of creating a new U.

#### groupWith(other, *others)
Alias for cogroup but with support for multiple RDDs.

     w = sc.parallelize([("a", 5), ("b", 6)])
     x = sc.parallelize([("a", 1), ("b", 4)])
     y = sc.parallelize([("a", 2)])
     z = sc.parallelize([("b", 42)])
     [(x, tuple(map(list, y))) for x, y in sorted(list(w.groupWith(x, y, z).collect()))]
     [('a', ([5], [1], [2], [])), ('b', ([6], [4], [], [42]))]


#### id()
A unique ID for this RDD (within its SparkContext).

#### intersection(other)
Return the intersection of this RDD and another one. The output will not contain any duplicate elements, even if the input RDDs did.

Note This method performs a shuffle internally.

     rdd1 = sc.parallelize([1, 10, 2, 3, 4, 5])
     rdd2 = sc.parallelize([1, 6, 2, 3, 7, 8])
     rdd1.intersection(rdd2).collect()
     [1, 2, 3]

#### checkpoint()
Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint directory set with SparkContext.setCheckpointDir() and all references to its parent RDDs will be removed. This function must be called before any job has been executed on this RDD. It is strongly recommended that this RDD is persisted in memory, otherwise saving it on a file will require recomputation.

#### setCheckpointDir(dirName)
Set the directory under which RDDs are going to be checkpointed. The directory must be a HDFS path if running on a cluster.

#### isCheckpointed()
Return whether this RDD is checkpointed and materialized, either reliably or locally.

#### isEmpty()
Returns true if and only if the RDD contains no elements at all.

Note an RDD may be empty even when it has at least 1 partition.

     sc.parallelize([]).isEmpty()
     True
     sc.parallelize([1]).isEmpty()
     False

#### isLocallyCheckpointed()
Return whether this RDD is marked for local checkpointing.

Exposed for testing.

#### join(other, numPartitions=None)
Return an RDD containing all pairs of elements with matching keys in self and other.

Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in self and (k, v2) is in other.

Performs a hash join across the cluster.

     x = sc.parallelize([("a", 1), ("b", 4)])
     y = sc.parallelize([("a", 2), ("a", 3)])
     sorted(x.join(y).collect())
     [('a', (1, 2)), ('a', (1, 3))]

#### keyBy(f)
Creates tuples of the elements in this RDD by applying f.

     x = sc.parallelize(range(0,3)).keyBy(lambda x: x*x)
     y = sc.parallelize(zip(range(0,5), range(0,5)))
     [(x, list(map(list, y))) for x, y in sorted(x.cogroup(y).collect())]
     [(0, [[0], [0]]), (1, [[1], [1]]), (2, [[], [2]]), (3, [[], [3]]), (4, [[2], [4]])]

#### keys()
Return an RDD with the keys of each tuple.

     m = sc.parallelize([(1, 2), (3, 4)]).keys()
     m.collect()
     [1, 3]

#### leftOuterJoin(other, numPartitions=None)
Perform a left outer join of self and other.

For each element (k, v) in self, the resulting RDD will either contain all pairs (k, (v, w)) for w in other, or the pair (k, (v, None)) if no elements in other have key k.

Hash-partitions the resulting RDD into the given number of partitions.

     x = sc.parallelize([("a", 1), ("b", 4)])
     y = sc.parallelize([("a", 2)])
     sorted(x.leftOuterJoin(y).collect())
     [('a', (1, 2)), ('b', (4, None))]

#### localCheckpoint()
Mark this RDD for local checkpointing using Spark’s existing caching layer.

This method is for users who wish to truncate RDD lineages while skipping the expensive step of replicating the materialized data in a reliable distributed file system. This is useful for RDDs with long lineages that need to be truncated periodically (e.g. GraphX).

Local checkpointing sacrifices fault-tolerance for performance. In particular, checkpointed data is written to ephemeral local storage in the executors instead of to a reliable, fault-tolerant storage. The effect is that if an executor fails during the computation, the checkpointed data may no longer be accessible, causing an irrecoverable job failure.

This is NOT safe to use with dynamic allocation, which removes executors along with their cached blocks. If you must use both features, you are advised to set spark.dynamicAllocation.cachedExecutorIdleTimeout to a high value.

The checkpoint directory set through SparkContext.setCheckpointDir() is not used.

#### lookup(key)
Return the list of values in the RDD for key key. This operation is done efficiently if the RDD has a known partitioner by only searching the partition that the key maps to.

     l = range(1000)
     rdd = sc.parallelize(zip(l, l), 10)
     rdd.lookup(42)  # slow
     [42]
     sorted = rdd.sortByKey()
     sorted.lookup(42)  # fast
     [42]
     sorted.lookup(1024)
     []
     rdd2 = sc.parallelize([(('a', 'b'), 'c')]).groupByKey()
     list(rdd2.lookup(('a', 'b'))[0])
     ['c']

#### map(f, preservesPartitioning=False)
Return a new RDD by applying a function to each element of this RDD.

     rdd = sc.parallelize(["b", "a", "c"])
     sorted(rdd.map(lambda x: (x, 1)).collect())
     [('a', 1), ('b', 1), ('c', 1)]

#### mapPartitions(f, preservesPartitioning=False)
Return a new RDD by applying a function to each partition of this RDD. mapPartitions() can be used as an alternative to map() & foreach(). mapPartitions() is called once for each Partition unlike map() & foreach() which is called for each element in the RDD. The main advantage being that, we can do initialization on Per-Partition basis instead of per-element basis(as done by map() & foreach())

Consider the case of Initializing a database. If we are using map() or foreach(), the number of times we would need to initialize will be equal to the no of elements in RDD. Whereas if we use mapPartitions(), the no of times we would need to initialize would be equal to number of Partitions

We get Iterator as an argument for mapPartition, through which we can iterate through all the elements in a Partition.

     rdd = sc.parallelize([1, 2, 3, 4], 2)
     def f(iterator): yield sum(iterator)
     rdd.mapPartitions(f).collect()
     [3, 7]

#### mapPartitionsWithIndex(f, preservesPartitioning=False)
Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original partition.

     rdd = sc.parallelize([1, 2, 3, 4], 2)
     def f(splitIndex, iterator): yield (splitIndex,sum(iterator))
     rdd.mapPartitionsWithIndex(f).collect()

#### max(key=None)
Find the maximum item in this RDD.

Parameters:	key – A function used to generate key for comparing

     rdd = sc.parallelize([1.0, 5.0, 43.0, 10.0])
     rdd.max()
     43.0
     rdd.max(key=str)
     5.0

#### mean()
Compute the mean of this RDD’s elements.

     sc.parallelize([1, 2, 3]).mean()
     2.0

#### min(key=None)
Find the minimum item in this RDD.

Parameters:	key – A function used to generate key for comparing

     rdd = sc.parallelize([2.0, 5.0, 43.0, 10.0])
     rdd.min()
     2.0
     rdd.min(key=str)
     10.0

#### name()
Return the name of this RDD.

#### partitionBy(numPartitions, partitionFunc=<function portable_hash at 0x7fc35dbc8e60>)
Return a copy of the RDD partitioned using the specified partitioner.

     pairs = sc.parallelize([1, 2, 3, 4, 2, 4, 1]).map(lambda x: (x, x))
     sets = pairs.partitionBy(2).glom().collect()
     len(set(sets[0]).intersection(set(sets[1])))
     0

#### persist(storageLevel=StorageLevel(False, True, False, False, 1))
Set this RDD’s storage level to persist its values across operations after the first time it is computed. This can only be used to assign a new storage level if the RDD does not have a storage level set yet. If no storage level is specified defaults to (MEMORY_ONLY).

     rdd = sc.parallelize(["b", "a", "c"])
     rdd.persist().is_cached
     True

#### pipe(command, env=None, checkCode=False)
Return an RDD created by piping elements to a forked external process.

     sc.parallelize(['1', '2', '', '3']).pipe('cat').collect()
     [u'1', u'2', u'', u'3']

Parameters:	checkCode – whether or not to check the return value of the shell command.

#### randomSplit(weights, seed=None)
Randomly splits this RDD with the provided weights.

Parameters:	
weights – weights for splits, will be normalized if they don’t sum to 1
seed – random seed
Returns:	
split RDDs in a list

     rdd = sc.parallelize(range(500), 1)
     rdd1, rdd2 = rdd.randomSplit([2, 3], 17)
     len(rdd1.collect() + rdd2.collect())
     500
     150 < rdd1.count() < 250
     True
     250 < rdd2.count() < 350
     True

#### reduce(f)
Reduces the elements of this RDD using the specified commutative and associative binary operator. Currently reduces partitions locally. Reduce(<function type>) takes a Function Type ; which takes 2 elements of RDD Element Type as argument & returns the Element of same type

     from operator import add
     sc.parallelize([1, 2, 3, 4, 5]).reduce(add)
     15
     sc.parallelize([1, 2, 3, 4, 5]).reduce(lambda x, y: x + y)

     A word of caution is necessary here. The functions passed as a reducer need to be associative, that is, when the      order of elements is changed the result does not, and commutative, that is, changing the order of operands does not change the result either.

     The example of the associativity rule is (5 + 2) + 3 = 5 + (2 + 3), and of the commutative is 5 + 2 + 3 = 3 + 2 + 5.    Thus, you need to be careful about what functions you pass to the reducer.

     If you ignore the preceding rule, you might run into trouble (assuming your code runs at all). For example, let's assume we have the following RDD (with one partition only!):

     data_reduce = sc.parallelize([1, 2, .5, .1, 5, .2], 1)
     If we were to reduce the data in a manner that we would like to divide the current result by the subsequent one, we would expect a value of 10:

     works = data_reduce.reduce(lambda x, y: x / y)
However, if you were to partition the data into three partitions, the result will be wrong:

     data_reduce = sc.parallelize([1, 2, .5, .1, 5, .2], 3)
     data_reduce.reduce(lambda x, y: x / y)
     It will produce 0.004.

### fold
fold() is similar to reduce except that it takes an 'Zero value'(Think of it as a kind of initial value) which will be used in the initial call on each Partition

    rdd = sc.parallelize([1, 2, 3, 4], 3)
    rdd.fold(3, lambda x,y: x +y)


#### reduceByKey(func, numPartitions=None, partitionFunc=<function portable_hash at 0x7fc35dbc8e60>)
Merge the values for each key using an associative and commutative reduce function.

This will also perform the merging locally on each mapper before sending results to a reducer, similarly to a “combiner” in MapReduce.

Output will be partitioned with numPartitions partitions, or the default parallelism level if numPartitions is not specified. Default partitioner is hash-partition.

     from operator import add
     rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
     sorted(rdd.reduceByKey(add).collect())
     [('a', 2), ('b', 1)]

#### repartition(numPartitions)
Return a new RDD that has exactly numPartitions partitions.

Can increase or decrease the level of parallelism in this RDD. Internally, this uses a shuffle to redistribute data. If you are decreasing the number of partitions in this RDD, consider using coalesce, which can avoid performing a shuffle.

     rdd = sc.parallelize([1,2,3,4,5,6,7], 4)
     sorted(rdd.glom().collect())
     [[1], [2, 3], [4, 5], [6, 7]]
     len(rdd.repartition(2).glom().collect())
     2
     len(rdd.repartition(10).glom().collect())
     10

#### repartitionAndSortWithinPartitions(numPartitions=None, partitionFunc=<function portable_hash at 0x7fc35dbc8e60>, ascending=True, keyfunc=<function <lambda> at 0x7fc35dbcf758>)
Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys.

     rdd = sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)])
     rdd2 = rdd.repartitionAndSortWithinPartitions(2, lambda x: x % 2, 2)
     rdd2.glom().collect()
     [[(0, 5), (0, 8), (2, 6)], [(1, 3), (3, 8), (3, 8)]]

#### rightOuterJoin(other, numPartitions=None)
Perform a right outer join of self and other.

For each element (k, w) in other, the resulting RDD will either contain all pairs (k, (v, w)) for v in this, or the pair (k, (None, w)) if no elements in self have key k.

Hash-partitions the resulting RDD into the given number of partitions.

     x = sc.parallelize([("a", 1), ("b", 4)])
     y = sc.parallelize([("a", 2)])
     sorted(y.rightOuterJoin(x).collect())
     [('a', (2, 1)), ('b', (None, 4))]

#### sample(withReplacement, fraction, seed=None)
Return a sampled subset of this RDD.

Parameters:	
withReplacement – can elements be sampled multiple times (replaced when sampled out)
fraction – expected size of the sample as a fraction of this RDD’s size without replacement: probability that each element is chosen; fraction must be [0, 1] with replacement: expected number of times each element is chosen; fraction must be >= 0
seed – seed for the random number generator
Note This is not guaranteed to provide exactly the fraction specified of the total count of the given DataFrame.

     rdd = sc.parallelize(range(100), 4)
     6 <= rdd.sample(False, 0.1, 81).count() <= 14
     True

#### sampleByKey(withReplacement, fractions, seed=None)
Return a subset of this RDD sampled by key (via stratified sampling). Create a sample of this RDD using variable sampling rates for different keys as specified by fractions, a key to sampling rate map.

     fractions = {"a": 0.2, "b": 0.1}
     rdd = sc.parallelize(fractions.keys()).cartesian(sc.parallelize(range(0, 1000)))
     sample = dict(rdd.sampleByKey(False, fractions, 2).groupByKey().collect())
     100 < len(sample["a"]) < 300 and 50 < len(sample["b"]) < 150
     True
     max(sample["a"]) <= 999 and min(sample["a"]) >= 0
     True
     max(sample["b"]) <= 999 and min(sample["b"]) >= 0
     True

#### sampleStdev()
Compute the sample standard deviation of this RDD’s elements (which corrects for bias in estimating the standard deviation by dividing by N-1 instead of N).

     sc.parallelize([1, 2, 3]).sampleStdev()
     1.0

#### sampleVariance()
Compute the sample variance of this RDD’s elements (which corrects for bias in estimating the variance by dividing by N-1 instead of N).

     sc.parallelize([1, 2, 3]).sampleVariance()
     1.0

#### saveAsHadoopDataset(conf, keyConverter=None, valueConverter=None)
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the old Hadoop OutputFormat API (mapred package). Keys/values are converted for output using either user specified converters or, by default, org.apache.spark.api.python.JavaToWritableConverter.

Parameters:	
conf – Hadoop job configuration, passed in as a dict
keyConverter – (None by default)
valueConverter – (None by default)

#### saveAsHadoopFile(path, outputFormatClass, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, conf=None, compressionCodecClass=None)
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the old Hadoop OutputFormat API (mapred package). Key and value types will be inferred if not specified. Keys and values are converted for output using either user specified converters or org.apache.spark.api.python.JavaToWritableConverter. The conf is applied on top of the base Hadoop conf associated with the SparkContext of this RDD to create a merged Hadoop MapReduce job configuration for saving the data.

Parameters:	
path – path to Hadoop file
outputFormatClass – fully qualified classname of Hadoop OutputFormat (e.g. “org.apache.hadoop.mapred.SequenceFileOutputFormat”)
keyClass – fully qualified classname of key Writable class (e.g. “org.apache.hadoop.io.IntWritable”, None by default)
valueClass – fully qualified classname of value Writable class (e.g. “org.apache.hadoop.io.Text”, None by default)
keyConverter – (None by default)
valueConverter – (None by default)
conf – (None by default)
compressionCodecClass – (None by default)

#### saveAsNewAPIHadoopDataset(conf, keyConverter=None, valueConverter=None)
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the new Hadoop OutputFormat API (mapreduce package). Keys/values are converted for output using either user specified converters or, by default, org.apache.spark.api.python.JavaToWritableConverter.

Parameters:	
conf – Hadoop job configuration, passed in as a dict
keyConverter – (None by default)
valueConverter – (None by default)

#### saveAsNewAPIHadoopFile(path, outputFormatClass, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, conf=None)
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the new Hadoop OutputFormat API (mapreduce package). Key and value types will be inferred if not specified. Keys and values are converted for output using either user specified converters or org.apache.spark.api.python.JavaToWritableConverter. The conf is applied on top of the base Hadoop conf associated with the SparkContext of this RDD to create a merged Hadoop MapReduce job configuration for saving the data.

Parameters:	
path – path to Hadoop file

#### outputFormatClass – fully qualified classname of Hadoop OutputFormat (e.g. “org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat”)
keyClass – fully qualified classname of key Writable class (e.g. “org.apache.hadoop.io.IntWritable”, None by default)
valueClass – fully qualified classname of value Writable class (e.g. “org.apache.hadoop.io.Text”, None by default)
keyConverter – (None by default)
valueConverter – (None by default)
conf – Hadoop job configuration, passed in as a dict (None by default)

#### saveAsPickleFile(path, batchSize=10)
Save this RDD as a SequenceFile of serialized objects. The serializer used is pyspark.serializers.PickleSerializer, default batch size is 10.

     tmpFile = NamedTemporaryFile(delete=True)
     tmpFile.close()
     sc.parallelize([1, 2, 'spark', 'rdd']).saveAsPickleFile(tmpFile.name, 3)
     sorted(sc.pickleFile(tmpFile.name, 5).map(str).collect())
     ['1', '2', 'rdd', 'spark']

#### saveAsSequenceFile(path, compressionCodecClass=None)
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the org.apache.hadoop.io.Writable types that we convert from the RDD’s key and value types. The mechanism is as follows:

Pyrolite is used to convert pickled Python RDD into RDD of Java objects.
Keys and values of this Java RDD are converted to Writables and written out.
Parameters:	
path – path to sequence file
compressionCodecClass – (None by default)
saveAsTextFile(path, compressionCodecClass=None)
Save this RDD as a text file, using string representations of elements.

Parameters:	
path – path to text file

#### compressionCodecClass – (None by default) string i.e. “org.apache.hadoop.io.compress.GzipCodec”

     tempFile = NamedTemporaryFile(delete=True)
     tempFile.close()
     sc.parallelize(range(10)).saveAsTextFile(tempFile.name)
     from fileinput import input
     from glob import glob
     ''.join(sorted(input(glob(tempFile.name + "/part-0000*"))))
'0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n'
Empty lines are tolerated when saving to text files.

     tempFile2 = NamedTemporaryFile(delete=True)
     tempFile2.close()
     sc.parallelize(['', 'foo', '', 'bar', '']).saveAsTextFile(tempFile2.name)
     ''.join(sorted(input(glob(tempFile2.name + "/part-0000*"))))
'\n\n\nbar\nfoo\n'
Using compressionCodecClass

     tempFile3 = NamedTemporaryFile(delete=True)
     tempFile3.close()
     codec = "org.apache.hadoop.io.compress.GzipCodec"
     sc.parallelize(['foo', 'bar']).saveAsTextFile(tempFile3.name, codec)
     from fileinput import input, hook_compressed
     result = sorted(input(glob(tempFile3.name + "/part*.gz"), openhook=hook_compressed))
     b''.join(result).decode('utf-8')
u'bar\nfoo\n'

#### setName(name)
Assign a name to this RDD.

     rdd1 = sc.parallelize([1, 2])
     rdd1.setName('RDD1').name()
     u'RDD1'

#### sortBy(keyfunc, ascending=True, numPartitions=None)
Sorts this RDD by the given keyfunc

     tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
     sc.parallelize(tmp).sortBy(lambda x: x[0]).collect()
     [('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
     sc.parallelize(tmp).sortBy(lambda x: x[1]).collect()
     [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]


#### stats()
Return a StatCounter object that captures the mean, variance and count of the RDD’s elements in one operation.

#### stdev()
Compute the standard deviation of this RDD’s elements.

     sc.parallelize([1, 2, 3]).stdev()
     0.816...

#### subtract(other, numPartitions=None)
Return each value in self that is not contained in other.

     x = sc.parallelize([("a", 1), ("b", 4), ("b", 5), ("a", 3)])
     y = sc.parallelize([("a", 3), ("c", None)])
     sorted(x.subtract(y).collect())
     [('a', 1), ('b', 4), ('b', 5)]

#### sum()
Add up the elements in this RDD.

     sc.parallelize([1.0, 2.0, 3.0]).sum()
     6.0

#### sumApprox(timeout, confidence=0.95)
Note Experimental
Approximate operation to return the sum within a timeout or meet the confidence.

     rdd = sc.parallelize(range(1000), 10)
     r = sum(range(1000))
     abs(rdd.sumApprox(1000) - r) / r < 0.05
     True

#### take(num)
Take the first num elements of the RDD.

It works by first scanning one partition, and use the results from that partition to estimate the number of additional partitions needed to satisfy the limit.

Translated from the Scala implementation in RDD#take().

Note this method should only be used if the resulting array is expected to be small, as all the data is loaded into the driver’s memory.

     sc.parallelize([2, 3, 4, 5, 6]).cache().take(2)
     [2, 3]
     sc.parallelize([2, 3, 4, 5, 6]).take(10)
     [2, 3, 4, 5, 6]
     sc.parallelize(range(100), 100).filter(lambda x: x > 90).take(3)
     [91, 92, 93]

#### takeOrdered(num, key=None)
Get the N elements from an RDD ordered in ascending order or as specified by the optional key function.

Note this method should only be used if the resulting array is expected to be small, as all the data is loaded into the driver’s memory.

     sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7]).takeOrdered(6)
     [1, 2, 3, 4, 5, 6]
     sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7], 2).takeOrdered(6, key=lambda x: -x)
     [10, 9, 7, 6, 5, 4]

#### takeSample(withReplacement, num, seed=None)
Return a fixed-size sampled subset of this RDD.

Note This method should only be used if the resulting array is expected to be small, as all the data is loaded into the driver’s memory.

     rdd = sc.parallelize(range(0, 10))
     len(rdd.takeSample(True, 20, 1))
     20
     len(rdd.takeSample(False, 5, 2))
     5
     len(rdd.takeSample(False, 15, 3))
     10

#### toDebugString()
A description of this RDD and its recursive dependencies for debugging.

#### top(num, key=None)
Get the top N elements from an RDD.

Note This method should only be used if the resulting array is expected to be small, as all the data is loaded into the driver’s memory.
Note It returns the list sorted in descending order.

     sc.parallelize([10, 4, 2, 12, 3]).top(1)
     [12]
     sc.parallelize([2, 3, 4, 5, 6], 2).top(2)
     [6, 5]
     sc.parallelize([10, 4, 2, 12, 3]).top(3, key=str)
     [4, 3, 2]

#### union(other)
Return the union of this RDD and another one.

     rdd = sc.parallelize([1, 1, 2, 3])
     rdd.union(rdd).collect()
     [1, 1, 2, 3, 1, 1, 2, 3]

#### unpersist()
Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.

#### variance()
Compute the variance of this RDD’s elements.

     sc.parallelize([1, 2, 3]).variance()
     0.666...

#### zip(other)
Zips this RDD with another one, returning key-value pairs with the first element in each RDD second element in each RDD, etc. Assumes that the two RDDs have the same number of partitions and the same number of elements in each partition (e.g. one was made through a map on the other).

     x = sc.parallelize(range(0,5))
     y = sc.parallelize(range(1000, 1005))
     x.zip(y).collect()
     [(0, 1000), (1, 1001), (2, 1002), (3, 1003), (4, 1004)]

#### zipWithIndex()
Zips this RDD with its element indices.

The ordering is first based on the partition index and then the ordering of items within each partition. So the first item in the first partition gets index 0, and the last item in the last partition receives the largest index.

This method needs to trigger a spark job when this RDD contains more than one partitions.

     sc.parallelize(["a", "b", "c", "d"], 3).zipWithIndex().collect()
     [('a', 0), ('b', 1), ('c', 2), ('d', 3)]

#### zipWithUniqueId()
Zips this RDD with generated unique Long ids.

Items in the kth partition will get ids k, n+k, 2*n+k, ..., where n is the number of partitions. So there may exist gaps, but this method won’t trigger a spark job, which is different from zipWithIndex

     sc.parallelize(["a", "b", "c", "d", "e"], 3).zipWithUniqueId().collect()
     [('a', 0), ('b', 1), ('c', 4), ('d', 2), ('e', 5)]


### Transformation on Pair RDD

Here, we talk about RDDs of key/value pairs, which are a common data type required for many operations in Spark. Key/value RDDs are commonly used to perform aggregations, and often we will do some initial ETL (extract, transform, and load) to get our data into a key/value format. Key/value RDDs expose new operations (e.g., counting up reviews for each product, grouping together data with the same key, and grouping together two different RDDs)

#### cogroup(other, numPartitions=None)
For each key k in self or other, return a resulting RDD that contains a tuple with the list of values for that key in self as well as other.

     x = sc.parallelize([("a", 1), ("b", 4)])
     y = sc.parallelize([("a", 2)])
     [(a, tuple(map(list, b))) for a, b in x.cogroup(y).collect()]
     [('a', ([1], [2])), ('b', ([4], []))]

#### values()
Return an RDD with the values of each tuple.

     m = sc.parallelize([(1, 2), (3, 4)]).values()
     m.collect()
     [2, 4]

#### subtractByKey(other, numPartitions=None)
Return each (key, value) pair in self that has no pair with matching key in other.

     x = sc.parallelize([("a", 1), ("b", 4), ("b", 5), ("a", 2)])
     y = sc.parallelize([("a", 3), ("c", None)])
     sorted(x.subtractByKey(y).collect())
     [('b', 4), ('b', 5)]

#### sortByKey(ascending=True, numPartitions=None, keyfunc=<function <lambda> at 0x7fc35dbcf848>)
Sorts this RDD, which is assumed to consist of (key, value) pairs. # noqa

     tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
     sc.parallelize(tmp).sortByKey().first()
     ('1', 3)
     sc.parallelize(tmp).sortByKey(True, 1).collect()
     [('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
     sc.parallelize(tmp).sortByKey(True, 2).collect()
     [('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
     tmp2 = [('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]
     tmp2.extend([('whose', 6), ('fleece', 7), ('was', 8), ('white', 9)])
     sc.parallelize(tmp2).sortByKey(True, 3, keyfunc=lambda k: k.lower()).collect()
     [('a', 3), ('fleece', 7), ('had', 2), ('lamb', 5),...('white', 9), ('whose', 6)]

#### mapValues(f)
Pass each value in the key-value pair RDD through a map function without changing the keys; this also retains the original RDD’s partitioning.

     x = sc.parallelize([("a", ["apple", "banana", "lemon"]), ("b", ["grapes"])])
     def f(x): return len(x)
     x.mapValues(f).collect()
     [('a', 3), ('b', 1)]