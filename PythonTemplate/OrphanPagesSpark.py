#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def process_one_line(line):
    source_str, targets_str = line.split(": ")
    source = int(source_str)
    targets = [int(x) for x in targets_str.split(" ")]
    return [(source, 0)] + [(target, 1) for target in targets]

orphan_pages = lines.flatMap(process_one_line).reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] == 0).map(lambda x: x[0]).sortBy(str).collect()

output = open(sys.argv[2], "w")

for page in orphan_pages:
    output.write("{}\n".format(page))

sc.stop()

