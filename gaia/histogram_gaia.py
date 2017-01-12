#!/usr/bin/env python

from __future__ import print_function
import sys
import numpy as np
import datetime

from pyspark import SparkContext, SparkConf
import pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.types

conf = SparkConf().setAppName("HistogramGaia")
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

sc.setLogLevel("WARN")

def simple_histogram(row):
    bins = np.linspace(10,15,15)
    H, _ = np.histogram(row.phot_g_mean_mag, bins=bins)
    return H

def partition_histogram(partition):
    bins = np.linspace(10,15,15)
    phot_g_mean_mag = np.fromiter((x.phot_g_mean_mag for x in partition),
                                 np.float)
    H, _ = np.histogram(phot_g_mean_mag, bins=bins)
    return H

print("Load start")
print(datetime.datetime.now())

df = spark.read.parquet("swift://cts-gaia.Nebula/gaia-dr1.parquet")
print("Load complete")
print(datetime.datetime.now())

#H = df.filter("htm_id=11056").select("phot_g_mean_mag").rdd.map(simple_histogram).sum()
H = df.select("phot_g_mean_mag").rdd.mapPartitions(partition_histogram).sum()

print("Histogram complete")
print(datetime.datetime.now())
print(H)

