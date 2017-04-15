#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning Borough (Column 23)
# Vales with "Unspecidief" are marked invalid.

import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str)
]

boroughSet=[
    "BROOKLYN",
    "MANHATTAN",
    "BRONX",
    "STATEN ISLAND",
    "QUEENS"
]


def getValid(Borough):
    pattern = re.compile("^(?:[A-Za-z0-9 ])+$")
    combined = re.compile("(" + ")|(".join(boroughSet) + ")")
    if Borough != "" and pattern.match(Borough) and combined.match((Borough)):
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
        if x == '':
            label = 'N/A'
        else:
            label = "invalid"
    else:
        label = "valid"
    typ = str(typ).replace('<class','').strip('>').strip(' ').strip('\'')
    return str(x + ', ' + str(typ) + ', ' + 'Borough Name, ' + label)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: Borough.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    boroughname = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[23])
    base_type=boroughname.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("Borough.txt")
sc.stop()

