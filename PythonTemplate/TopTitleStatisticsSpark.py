#!/usr/bin/env python
import sys
import math
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

counts = lines.map(lambda line: int(line.split("\t")[1])).collect()

ans_max = max(counts)
ans_min = min(counts)
ans_sum = sum(counts)
ans_mean = ans_sum // len(counts)
ans_var = sum([(x - ans_mean) ** 2 for x in counts]) // len(counts)

outputFile = open(sys.argv[2], "w")

outputFile.write('Mean\t%s\n' % ans_mean)
outputFile.write('Sum\t%s\n' % ans_sum)
outputFile.write('Min\t%s\n' % ans_min)
outputFile.write('Max\t%s\n' % ans_max)
outputFile.write('Var\t%s\n' % ans_var)

sc.stop()

