#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning Complaints Type (Column 5)
# All entries with numeric values and special characters except for
# '-' and '/' are invalid.


import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str)
]


def getValid(ComplaintType):
    pattern = re.compile('^(?:[A-Z]|[a-z]|/|\s|-)+$')
    if ComplaintType != "" and pattern.match(ComplaintType):
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
    return str(x + ', ' + str(typ) + ', ' + 'Complaint Type, ' + label)
    return x, val



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ComplaintType.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    complaint = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[5])
    base_type=complaint.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("ComplaintType.txt")
sc.stop()
