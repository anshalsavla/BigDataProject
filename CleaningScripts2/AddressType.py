#!/usr/bin/env python


import sys
from pyspark import SparkContext
import csv

# Cleaning Address Type1 (Column 15)

import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str)
]



def getValid(AddressType):
    pattern = re.compile("^(?:[A-Z])+$")
    if AddressType != '' or pattern.match(AddressType):
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
    return str(x + ', ' + str(typ) + ', ' + 'Address Type, ' + label)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: AddressType.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    addr = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[15])
    base_type=addr.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("AddressType.txt")
    sc.stop()
