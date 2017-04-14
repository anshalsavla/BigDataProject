#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning Landmark (Column 17)

import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str)
]


def getValid(Landmark):
    pattern = re.compile("^(?:[A-Z0-9 \/])+$")
    if Landmark == "" or pattern.match(Landmark):
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
    return str(x + ', ' + str(typ) + ', ' + 'Landmark, ' + label)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: Landmark.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    street = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[17])
    base_type=street.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("Landmark.txt")
sc.stop()

