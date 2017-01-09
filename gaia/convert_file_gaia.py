
import sys
from astropy.io import fits
import pandas
import numpy as np
from pyspark import SparkContext, SparkConf
import pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.types
from esutil.htm import HTM
import glob

conf = SparkConf().setMaster("local[8]").setAppName("testJunk")
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

sc.setLogLevel("WARN")

def process_file(filename):

    f = fits.open(filename)
    table = f[1].data

    htm_index = HTM(depth=5)
    pandas_df = pandas.DataFrame(table)
    pandas_df['htm_id'] = htm_index.lookup_id(table['ra'], table['dec'])

    print "Finished file {:s}".format(filename)
    return pandas_df

def pandas_to_data_list(pandas_data):
    return [r.tolist() for r in pandas_data.to_records(index=False)]


def group_items(items, group_length):
    for n in xrange(0, len(items), group_length):
        yield items[n:(n+group_length)]


prefixes = range(0,21)

for prefix_n in prefixes:
    filenames = glob.glob("/stripe82/gaia_source/GaiaSource_000-0{:02d}-*.fits".format(prefix_n))

    filename_rdd = sc.parallelize(filenames, numSlices=80)
    pandas_rdd = filename_rdd.map(process_file)

    schema = [str(x) for x in pandas_rdd.first().columns]
    data_rdd = pandas_rdd.flatMap(pandas_to_data_list) #.partition(10)

    df = spark.createDataFrame(data_rdd, schema=schema, verifySchema=False)

    df.write.parquet("swift://cts-gaia.Nebula/gaia-dr1-{:02d}.parquet".format(prefix_n),
                     partitionBy=['htm_id'])
                     #mode="append")

sc.stop()



