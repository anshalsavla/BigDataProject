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
    "N/A",
    "Ramp",
    "Roadway"
]
def getDataType(x):
    label = "invalid"

    for typ, test in tests:
        try:
            test(x)
            if typ == str:
                pattern = re.compile("(" + ")|(".join(regexes) + ")")
                if pattern.match(x):
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

    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+'Road/Ramp, '+label)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: RoadRamp.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    facility = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[44])
    base_type=facility.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("RoadRamp.txt")
    sc.stop()
