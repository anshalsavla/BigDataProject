#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning Intersection Street 1 (Column 13)

import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str)
]


def getValid(IntersectionStreet1):
    pattern = re.compile("^(?:[A-Z0-9 \.\/&\'-])+$")
    if IntersectionStreet1 == '' or pattern.match(IntersectionStreet1):
        return True
    return False


def getDataType(x):
    # label = "invalid"
    myVal = getValid(x)
    if myVal:
        typ = "str"
    else:
        for typ, test in tests:
            try:
                test(x)
            except ValueError:
                continue
    # return(myVal)
    if not myVal:
        label = "invalid"
    else:
        label = "valid"
    typ = str(typ).replace('<class','').strip('>').strip(' ').strip('\'')
    return str(x + ', ' + str(typ) + ', ' + 'Intersection Street No. 1, ' + label)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: IntersectionStreet1.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    street = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[13])
    base_type=street.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("IntersectionStreet1.txt")
sc.stop()
