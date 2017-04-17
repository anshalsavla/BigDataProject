#!/usr/bin/env python

#script find to number of occurences for each agency

import sys
from operator import add
from pyspark import SparkContext
import csv

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: agencyscript.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1], 1)

    agency = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[3]))
    agencycount = agency.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b) 
    agencycount.saveAsTextFile("agencyscript.txt")
    sc.stop()

