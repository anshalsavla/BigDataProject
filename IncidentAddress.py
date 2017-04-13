#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning Complaints Type (Column 9)
# All entries with blanks or special characters are invalid.


import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str)
]


def getValid(IncidentAddress):
    pattern = re.compile('^(?:[A-Z0-9 -])+$')
    if IncidentAddress != "" and pattern.match(IncidentAddress):
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
    return str(x + ', ' + str(typ) + ', ' + 'Incident Address Description, ' + label)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: IncidentAddress.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    incidentAddress = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[9])
    base_type=incidentAddress.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("IncidentAddress.txt")
sc.stop()
