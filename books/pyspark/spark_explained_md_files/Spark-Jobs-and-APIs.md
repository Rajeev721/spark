### Execution process
* Any Spark application spins off a single driver process (that can contain multiple jobs) on the master node that then directs executor processes (that contain multiple tasks) distributed to a number of worker nodes.
* The driver process determines the number and the composition of the task processes directed to the executor nodes based on the graph generated for the given job. Note, that any worker node can execute tasks from a number of different jobs.

![execution process](https://www.safaribooksonline.com/library/view/learning-pyspark/9781786463708/graphics/B05793_01_02.jpg)

* A Spark job is associated with a chain of object dependencies organized in a direct acyclic graph (DAG) such as the following example generated from the Spark UI. Given this, Spark can optimize the scheduling (for example, determine the number of tasks and workers required) and execution of these tasks.

![dag](https://www.safaribooksonline.com/library/view/learning-pyspark/9781786463708/graphics/B05793_01_03.jpg)

### Resilient Distributed Dataset
* Apache Spark is built around a distributed collection of immutable Java Virtual Machine (JVM) objects called Resilient Distributed Datasets (RDDs for short). 
* As we are working with Python, it is important to note that the Python data is stored within these JVM objects. 
* These objects allow any job to perform calculations very quickly. RDDs are calculated against, cached, and stored in-memory: a scheme that results in orders of magnitude faster computations compared to other traditional distributed frameworks like Apache Hadoop.
* RDDs expose some coarse-grained transformations (such as map(...), reduce(...), and filter(...) which we will cover in greater detail in Chapter 2, Resilient Distributed Datasets), keeping the flexibility and extensibility of the Hadoop platform to perform a wide variety of calculations. 
* RDDs apply and log transformations to the data in parallel, resulting in both increased speed and fault-tolerance. By registering the transformations, RDDs provide data lineage - a form of an ancestry tree for each intermediate step in the form of a graph. This, in effect, guards the RDDs against data loss - if a partition of an RDD is lost it still has enough information to recreate that partition instead of simply depending on replication.
* RDDs have two sets of parallel operations: transformations (which return pointers to new RDDs) and actions (which return values to the driver after running a computation)
* RDD transformation operations are lazy in a sense that they do not compute their results immediately. The transformations are only computed when an action is executed and the results need to be returned to the driver. This delayed execution results in more fine-tuned queries: Queries that are optimized for performance. 

### DataFrames
* DataFrames, like RDDs, are immutable collections of data distributed among the nodes in a cluster. However, unlike RDDs, in DataFrames data is organized into named columns.
* DataFrames were designed to make large data sets processing even easier. They allow developers to formalize the structure of the data, allowing higher-level abstraction; in that sense DataFrames resemble tables from the relational database world. DataFrames provide a domain specific language API to manipulate the distributed data and make Spark accessible to a wider audience, beyond specialized data engineers.
* One of the major benefits of DataFrames is that the Spark engine initially builds a logical execution plan and executes generated code based on a physical plan determined by a cost optimizer. Unlike RDDs that can be significantly slower on Python compared with Java or Scala, the introduction of DataFrames has brought performance parity across all the languages.