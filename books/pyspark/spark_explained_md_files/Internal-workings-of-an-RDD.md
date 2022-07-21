* RDDs operate in parallel. This is the strongest advantage of working in Spark: Each transformation is executed in parallel for enormous increase in speed.
* The transformations to the dataset are lazy. This means that any transformation is only executed when an action on a dataset is called. This helps Spark to optimize the execution. 
* For instance, consider the following very common steps that an analyst would normally do to get familiar with a dataset:

  - Count the occurrence of distinct values in a certain column.
  - Select those that start with an A.
  - Print the results to the screen.

* As simple as the previously mentioned steps sound, if only items that start with the letter A are of interest, there is no point in counting distinct values for all the other items. Thus, instead of following the execution as outlined in the preceding points, Spark could only count the items that start with A, and then print the results to the screen.