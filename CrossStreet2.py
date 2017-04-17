#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning Cross Street 2 (Column 12)

import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str)
]


def getValid(CrossStreet2):
    pattern = re.compile("^(?:[A-Z0-9 \.\/&\'-])+$")
    if CrossStreet2 != "" and pattern.match(CrossStreet2):
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
    return str(x + ', ' + str(typ) + ', ' + 'Cross Street No. 2, ' + label)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: CrossStreet2.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    street = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[12])
    base_type=street.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("CrossStreet2.txt")
    sc.stop()

