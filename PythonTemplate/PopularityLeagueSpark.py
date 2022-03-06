#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext
from collections import defaultdict

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def process_one_line(line):
    source, targets_str = line.split(": ")
    targets = targets_str.split(" ")
    return [(source, 0)] + [(target, 1) for target in targets if target != source]


leagueIds = sc.textFile(sys.argv[2], 1)
leagueIdsSet = set(leagueIds.collect())

page_counts = lines\
    .flatMap(process_one_line)\
    .reduceByKey(lambda a, b: a + b)\
    .filter(lambda x: x[0] in leagueIdsSet)\
    .collect()

ranks = defaultdict(int)
for page1, count1 in page_counts:
    for page2, count2 in page_counts:
        if count1 > count2:
            ranks[page1] += 1

output = open(sys.argv[3], "w")

# output.write("{}\n".format(leagueIdsSet))
# output.write("{}\n".format(page_counts))
# output.write("{}\n".format(ranks))
for page, count in sorted(page_counts, key=lambda x: x[0]):
    output.write("{}\t{}\n".format(page, ranks[page]))

sc.stop()

