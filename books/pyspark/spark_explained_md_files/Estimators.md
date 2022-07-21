* Estimators can be thought of as statistical models that need to be estimated to make predictions or classify your observations.
* If deriving from the abstract Estimator class, the new model has to implement the .fit(...) method that fits the model given the data found in a DataFrame and some default or user-specified parameters.

### Classification 

* LogisticRegression: The benchmark model for classification. The logistic regression uses a logit function to calculate the probability of an observation belonging to a particular class. At the time of writing, the PySpark ML supports only binary classification problems.
* DecisionTreeClassifier: A classifier that builds a decision tree to predict a class for an observation. Specifying the maxDepth parameter limits the depth the tree grows, the minInstancePerNode determines the minimum number of observations in the tree node required to further split, the maxBins parameter specifies the maximum number of bins the continuous variables will be split into, and the impurity specifies the metric to measure and calculate the information gain from the split.
* GBTClassifier: A Gradient Boosted Trees model for classification. The model belongs to the family of ensemble models: models that combine multiple weak predictive models to form a strong one. At the moment, the GBTClassifier model supports binary labels, and continuous and categorical features.
* RandomForestClassifier: This model produces multiple decision trees (hence the name—forest) and uses the mode output of those decision trees to classify observations. The RandomForestClassifier supports both binary and multinomial labels.
* NaiveBayes: Based on the Bayes' theorem, this model uses conditional probability theory to classify observations. The NaiveBayes model in PySpark ML supports both binary and multinomial labels.
* MultilayerPerceptronClassifier: A classifier that mimics the nature of a human brain. Deeply rooted in the Artificial Neural Networks theory, the model is a black-box, that is, it is not easy to interpret the internal parameters of the model. The model consists, at a minimum, of three, fully connected layers (a parameter that needs to be specified when creating the model object) of artificial neurons: the input layer (that needs to be equal to the number of features in your dataset), a number of hidden layers (at least one), and an output layer with the number of neurons equal to the number of categories in your label. All the neurons in the input and hidden layers have a sigmoid activation function, whereas the activation function of the neurons in the output layer is softmax.
* OneVsRest: A reduction of a multiclass classification to a binary one. For example, in the case of a multinomial label, the model can train multiple binary logistic regression models. For example, if label == 2, the model will build a logistic regression where it will convert the label == 2 to 1 (all remaining label values would be set to 0) and then train a binary model. All the models are then scored and the model with the highest probability wins.

### Regression
* AFTSurvivalRegression: Fits an Accelerated Failure Time regression model. It is a parametric model that assumes that a marginal effect of one of the features accelerates or decelerates a life expectancy (or process failure). It is highly applicable for the processes with well-defined stages.
* DecisionTreeRegressor: Similar to the model for classification with an obvious distinction that the label is continuous instead of binary (or multinomial).
* GBTRegressor: As with the DecisionTreeRegressor, the difference is the data type of the label.
GeneralizedLinearRegression: A family of linear models with differing kernel functions (link functions). In contrast to the linear regression that assumes normality of error terms, the GLM allows the label to have different error term distributions: the GeneralizedLinearRegression model from the PySpark ML package supports gaussian, binomial, gamma, and poisson families of error distributions with a host of different link functions.
* IsotonicRegression: A type of regression that fits a free-form, non-decreasing line to your data. It is useful to fit the datasets with ordered and increasing observations.
* LinearRegression: The most simple of regression models, it assumes a linear relationship between features and a continuous label, and normality of error terms.
* RandomForestRegressor: Similar to either DecisionTreeRegressor or GBTRegressor, the RandomForestRegressor fits a continuous label instead of a discrete one.

### Clustering
* BisectingKMeans: A combination of the k-means clustering method and hierarchical clustering. The algorithm begins with all observations in a single cluster and iteratively splits the data into k clusters.
* KMeans: This is the famous k-mean algorithm that separates data into k clusters, iteratively searching for centroids that minimize the sum of square distances between each observation and the centroid of the cluster it belongs to.
* GaussianMixture: This method uses k Gaussian distributions with unknown parameters to dissect the dataset. Using the Expectation-Maximization algorithm, the parameters for the Gaussians are found by maximizing the log-likelihood function.
* LDA: This model is used for topic modeling in natural language processing applications.
