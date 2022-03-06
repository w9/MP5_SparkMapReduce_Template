#!/usr/bin/env python
'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext
from collections import Counter

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
    stopWordsList = [x.strip() for x in f.readlines()]

with open(delimitersPath) as f:
    delimiters = f.read()

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)


def process_one_line(line):
    line_ascii = line
    # line_ascii = line.encode('ascii', 'replace').decode('utf-8')
    line_s = line_ascii.strip().lower()
    for dmt in delimiters:
        line_s = line_s.replace(dmt, " ")
    return [(x, 1) for x in line_s.split() if x not in stopWordsList]


outputFile = open(sys.argv[4], "w")

count_dict = lines.flatMap(process_one_line).countByKey()
for x, y in sorted(Counter(count_dict).most_common(10), key=lambda x: x[0]):
    outputFile.write("{}\t{}\n".format(x, y))

sc.stop()
