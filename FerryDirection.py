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
    (str, str),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d"))
]

regexes = [
    "Manhattan Bound",
    "Staten Island Bound",
    "N/A"
]
def getDataType(x):
    label = "invalid"

    for typ, test in tests:
        try:
            test(x)
            if typ == str:
                combined = re.compile("(" + ")|(".join(regexes) + ")")
                if combined.match(x):
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

    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+'Ferry Direction, '+label)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: FerryDirection.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    facility = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[47])
    base_type=facility.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("FerryDirection.txt")
sc.stop()

