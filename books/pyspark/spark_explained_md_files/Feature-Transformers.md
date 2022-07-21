* The Transformer class, like the name suggests, transforms your data by (normally) appending a new column to your DataFrame.
* At the high level, when deriving from the Transformer abstract class, each and every new Transformer needs to implement a .transform(...) method. 
* The method, as a first and normally the only obligatory parameter, requires passing a DataFrame to be transformed. 
* This, of course, varies method-by-method in the ML package: other popular parameters are inputCol and outputCol; these, however, frequently default to some predefined values, such as, for example, 'features' for the inputCol parameter.

### Transformers defined in spark.ml

* Binarizer: Given a threshold, the method takes a continuous variable and transforms it into a binary one.
* Bucketizer: Similar to the Binarizer, this method takes a list of thresholds (the splits parameter) and transforms a continuous variable into a multinomial one.
* ChiSqSelector: For the categorical target variables (think classification models), this feature allows you to select a predefined number of features (parameterized by the numTopFeatures parameter) that explain the variance in the target the best. The selection is done, as the name of the method suggests, using a Chi-Square test. It is one of the two-step methods: first, you need to .fit(...) your data (so the method can calculate the Chi-square tests). Calling the .fit(...) method (you pass your DataFrame as a parameter) returns a ChiSqSelectorModel object that you can then use to transform your DataFrame using the .transform(...) method.
* CountVectorizer: This is useful for a tokenized text (such as [['Learning', 'PySpark', 'with', 'us'],['us', 'us', 'us']]). It is one of two-step methods: first, you need to .fit(...), that is, learn the patterns from your dataset, before you can .transform(...) with the CountVectorizerModel returned by the .fit(...) method. The output from this transformer, for the tokenized text presented previously, would look similar to this: [(4, [0, 1, 2, 3], [1.0, 1.0, 1.0, 1.0]),(4, [3], [3.0])].
DCT: The Discrete Cosine Transform takes a vector of real values and returns a vector of the same length, but with the sum of cosine functions oscillating at different frequencies. Such transformations are useful to extract some underlying frequencies in your data or in data compression.
* ElementwiseProduct: A method that returns a vector with elements that are products of the vector passed to the method, and a vector passed as the scalingVec parameter. For example, if you had a [10.0, 3.0, 15.0] vector and your scalingVec was [0.99, 3.30, 0.66], then the vector you would get would look as follows: [9.9, 9.9, 9.9].
* HashingTF: A hashing trick transformer that takes a list of tokenized text and returns a vector (of predefined length) with counts. From PySpark's documentation:
"Since a simple modulo is used to transform the hash function to a column index, it is advisable to use a power of two as the numFeatures parameter; otherwise the features will not be mapped evenly to the columns."
* IDF: This method computes an Inverse Document Frequency for a list of documents. Note that the documents need to already be represented as a vector (for example, using either the HashingTF or CountVectorizer).
* IndexToString: A complement to the StringIndexer method. It uses the encoding from the StringIndexerModel object to reverse the string index to original values. As an aside, please note that this sometimes does not work and you need to specify the values from the StringIndexer.
* MaxAbsScaler: Rescales the data to be within the [-1.0, 1.0] range (thus, it does not shift the center of the data).
* MinMaxScaler: This is similar to the MaxAbsScaler with the difference that it scales the data to be in the [0.0, 1.0] range.
* NGram: This method takes a list of tokenized text and returns n-grams: pairs, triples, or n-mores of subsequent words. For example, if you had a ['good', 'morning', 'Robin', 'Williams'] vector you would get the following output: ['good morning', 'morning Robin', 'Robin Williams'].
* Normalizer: This method scales the data to be of unit norm using the p-norm value (by default, it is L2).
* OneHotEncoder: This method encodes a categorical column to a column of binary vectors.
* PCA: Performs the data reduction using principal component analysis.
* PolynomialExpansion: Performs a polynomial expansion of a vector. For example, if you had a vector symbolically written as [x, y, z], the method would produce the following expansion: [x, x*x, y, x*y, y*y, z, x*z, y*z, z*z].
* QuantileDiscretizer: Similar to the Bucketizer method, but instead of passing the splits parameter, you pass the numBuckets one. The method then decides, by calculating approximate quantiles over your data, what the splits should be.
* RegexTokenizer: This is a string tokenizer using regular expressions.
* RFormula: For those of you who are avid R users, you can pass a formula such as vec ~ alpha * 3 + beta (assuming your DataFrame has the alpha and beta columns) and it will produce the vec column given the expression.
* SQLTransformer: Similar to the previous, but instead of R-like formulas, you can use SQL syntax.
* StandardScaler: Standardizes the column to have a 0 mean and standard deviation equal to 1.
* StopWordsRemover: Removes stop words (such as 'the' or 'a') from a tokenized text.
* StringIndexer: Given a list of all the words in a column, this will produce a vector of indices.
* Tokenizer: This is the default tokenizer that converts the string to lower case and then splits on space(s).
* VectorAssembler: This is a highly useful transformer that collates multiple numeric (vectors included) columns into a single column with a vector representation. 
* VectorIndexer: This is a method for indexing categorical columns into a vector of indices. It works in a column-by-column fashion, selecting distinct values from the column, sorting and returning an index of the value from the map instead of the original value.
* VectorSlicer: Works on a feature vector, either dense or sparse: given a list of indices, it extracts the values from the feature vector.
* Word2Vec: This method takes a sentence (string) as an input and transforms it into a map of {string, vector} format, a representation that is useful in natural language processing.