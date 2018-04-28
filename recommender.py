from __future__ import print_function

import sys
import pandas

if sys.version >= '3':
    long = int

from pyspark.sql import SparkSession

# $example on$
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.types import *

import numpy as np
from pyspark.sql.functions import array, create_map, struct
from pyspark import SQLContext


# $example off$

def g(x):
    print(x)


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("ALSExample") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/tcc.ratings") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/tcc.recommendations") \
        .config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.10:2.2.1") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    print("Reading ratings from mongo")
    ratings = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

    print("Creating bases training and test")
    (training, test) = ratings.randomSplit([0.8, 0.2])

    print("Build the recommendation model using ALS on the training data")
    als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
              coldStartStrategy="drop")
    model = als.fit(training)

    print("Evaluate the model by computing the RMSE on the test data")
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))

    print("Generate top 10 movie recommendations for each user")
    userRecs = model.recommendForAllUsers(10)

    userRecs.select(userRecs["userId"], \
                    userRecs["recommendations"]["movieId"].alias("movieId"), \
                    userRecs["recommendations"]["rating"].cast('array<double>').alias("rating")). \
        write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

    spark.stop()