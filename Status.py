#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning Status (Column 19)

import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str)
]

statusCodes=[
    "Assigned",
    "Closed",
    "Draft",
    "Open",
    "Pending",
    "Started",
    "Unspecified"
]

def getValid(Status):
    combined = re.compile("(" + ")|(".join(statusCodes) + ")")
    if Status != "" and combined.match(Status):
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
    return str(x + ', ' + str(typ) + ', ' + 'Status Type, ' + label)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: Status.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    street = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[19])
    base_type=street.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("Status.txt")
    sc.stop()

