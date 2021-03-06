#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv
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

def getDataType(x):
    label = "invalid"

    for typ, test in tests:
        try:
            test(x)
            if typ == str:
                combined = re.compile("(" + ")|(".join(boroughSet) + ")")
                if combined.match(x):
                    label = "valid"
                    break
                else:
                    if x == '' or x == 'Unspecified':
                        label = "N/A"
                    else:
                        label = "invalid"
                    break;
            if typ == int:
                break
            if typ == float:
                break
            else:
                break

        except ValueError:
            continue

    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+'Taxi Borough, '+label)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: TaxiCompanyBorough.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    boroughname = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[40])
    base_type=boroughname.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("TaxiCompanyBorough.txt")
    sc.stop()
