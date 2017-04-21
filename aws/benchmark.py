from __future__ import print_function

import sys
import numpy as np
import json
import urllib2
import boto3
import collections
import datetime
import cPickle
from operator import add

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


def get_benchmark_results(start_time, end_time):
    user_data = json.load(urllib2.urlopen("http://169.254.169.254/latest/user-data/"))
    cluster_id = user_data['clusterId']

    emr = boto3.client("emr")
    instances = emr.list_instances(ClusterId=cluster_id)

    ig_counter = collections.Counter([x['InstanceGroupId'] for x in instances['Instances']])
    worker_instance_group = ig_counter.most_common()[0][0]

    worker_instance_ids = [x['Ec2InstanceId'] for x in instances['Instances']
                           if x['InstanceGroupId'] == worker_instance_group]

    cloudwatch = boto3.client('cloudwatch')
    stats = []
    for worker_id in worker_instance_ids:
        cpu_stats = cloudwatch.get_metric_statistics(Namespace="AWS/EC2", MetricName="CPUUtilization",
                    StartTime=start_time, EndTime=end_time, Period=60, Statistics=["Average"],
                    Dimensions=[{"Name": "InstanceId", "Value": worker_id}])
        stats.append(cpu_stats)


    s3_stats = cloudwatch.get_metric_statistics(Namespace="AWS/S3", MetricName="BytesDownloaded",
            StartTime=start_time, EndTime=end_time, Period=60, Statistics=["Sum","SampleCount","Average"],
            Dimensions=[{"Name": "FilterId", "Value": "EntireBucket"},
            {"Name": "BucketName", "Value": "gaia-dr1-ctslater"}])
    stats.append(s3_stats)

    return stats


def spatial_histogram(partition):

    coord_array = np.asarray( [(x.ra, x.dec) for x in partition],
                             dtype=(np.float, np.float))
    ra_bins = np.linspace(0,360,500)
    dec_bins = np.linspace(-90,90,250)
    if len(coord_array) == 0:
        return []

    H, _ = np.histogramdd(coord_array, bins=(ra_bins, dec_bins))

    # This must return an iterable to spark.
    # If the entire partition reduces to one object, that object
    # must be inside a list.
    return [H]

def sparse_spatial_histogram(partition):

    coord_array = np.asarray( [(x.ra, x.dec) for x in partition],
                             dtype=(np.float, np.float))
    ra_bins = np.linspace(0,360,500)
    dec_bins = np.linspace(-90,90,250)
    if len(coord_array) == 0:
        return []

    H, _ = np.histogramdd(coord_array, bins=(ra_bins, dec_bins))

    H_flat = H.flatten()
    sel, = np.where(H_flat > 0)

    # This must return an iterable to spark.
    # If the entire partition reduces to one object, that object
    # must be inside a list.
    return zip(sel, H_flat[sel])


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Must supply output filename")
        sys.exit(0)
    print(sys.argv[1])

    conf = SparkConf().setAppName("BenchmarkGaia")
    sc = SparkContext(conf = conf)
    sc.setLogLevel("WARN")
    spark = SparkSession(sc)
    df = spark.read.parquet("s3://gaia-dr1-ctslater/gaia-dr1.parquet")

    start_time = datetime.datetime.now()
    #H_map = df.select("ra", "dec").filter(df['htm_id'] < 10100).rdd.mapPartitions(spatial_histogram).sum()
    #H_map = df.select("ra", "dec").rdd.mapPartitions(spatial_histogram).sum()
    rdd = df.select("ra", "dec").rdd
    H_entries = rdd.mapPartitions(sparse_spatial_histogram).reduceByKey(add).collect()
    end_time = datetime.datetime.now()


    cpu_stats = get_benchmark_results(start_time, end_time)
    f_out = open(sys.argv[1], "w")
    cPickle.dump(cpu_stats, f_out)
    f_out.close()

