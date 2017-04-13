#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning Complaints Type (Column 7)
# All entries with blanks or special characters except for
# '-' '.' ',' '+' '(' ')' and '/' are invalid.


import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str)
]


def getValid(Location):
    pattern = re.compile('^(?:[A-Z]|[a-z]|[0-9]|/|\s|\(|\)|,|\+|\.|-)+$')
    if Location != "" and pattern.match(Location):
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
        typ = str(typ).strip('<type').strip('>').strip(' ').strip('\'')
    return str(x + ', ' + str(typ) + ', ' + 'Location Description, ' + label)
    


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: Location.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    location = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[7])
    base_type=location.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("Location.txt")
sc.stop()
