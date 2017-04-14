#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning City Name (Column 16)

import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str)
]



def getValid(City):
    pattern = re.compile("^(?:[A-Z])+$")
    if City != '' or pattern.match(City):
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
    return str(x + ', ' + str(typ) + ', ' + 'City Name, ' + label)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: City.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    addr = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[16])
    base_type=addr.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("City.txt")
sc.stop()
