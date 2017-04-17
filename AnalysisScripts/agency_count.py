#!/usr/bin/env python

#script find to number of occurences for each agency

import sys
from operator import add
from pyspark import SparkContext
import csv

def toCSVLine(data):
  return ','.join(str(d) for d in data)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: agencyscript <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1], 1)

    agency = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[3]))
    agencycount = agency.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    csvresult = agencycount.map(toCSVLine)
    csvresult.saveAsTextFile("agency_count.csv")
    sc.stop()
