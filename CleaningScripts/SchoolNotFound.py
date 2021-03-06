#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning SchoolNotFound (Column 37)
# Vales with "Unspecified" are marked invalid.

import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d"))

]

regexes=[
    "N",
    "Y",
    "Unspecified"
]

def getDataType(x):
    label = "invalid"

    for typ, test in tests:
        try:
            test(x)
            if typ == str:
                pattern = re.compile("^(?:[A-Za-z])+$")
                combined = re.compile("(" + ")|(".join(regexes) + ")")
                if pattern.match(x) and combined.match(x):
                    label = "valid"
                    break
                else:
                    if x == '':
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

    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+'School Not Found, '+label)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: SchoolNotFound.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    boroughname = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[37])
    base_type=boroughname.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("SchoolNotFound.txt")
    sc.stop()
