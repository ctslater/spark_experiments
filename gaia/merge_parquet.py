#!/usr/bin/env python

import sys
import numpy as np

from pyspark import SparkContext, SparkConf
import pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.types

conf = SparkConf().setAppName("MergeGaia")
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

sc.setLogLevel("WARN")

filename_template = "swift://cts-gaia.Nebula/gaia-dr1-{:02d}.parquet"

# Goal is to accomplish something like:
# df = df0.union(df1).union(df2).union(df3)

dfs =  [spark.read.parquet(filename_template.format(n))
        for n in range(0,21)]
df = reduce(lambda a,b: a.union(b), dfs)

df.write.parquet("swift://cts-gaia.Nebula/gaia-dr1.parquet",
                partitionBy=['htm_id'])

